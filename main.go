// nas-dmesg-watcher watches the kernel log (/dev/kmsg) for storage-related
// errors (bcachefs, libata, SCSI) and publishes alerts with surrounding
// context to MQTT, where Home Assistant picks them up via autodiscovery.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	contextBefore  = 3
	contextAfter   = 10
	collectTimeout = 5 * time.Second
	maxSummaryLen  = 240
)

var defaultPatterns = []string{
	`bcachefs.*(error|disconnected|inconsistency|emergency|fatal|shutting down|fsck)`,
	`ata\d+(\.\d+)?: (exception|interface fatal|failed to IDENTIFY|revalidation failed|disable device|hard resetting|limiting SATA link speed|link is slow)`,
	`FAILED Result: hostbyte=DID_`,
	`I/O error, dev `,
	`blk_update_request.*I/O error`,
	`Buffer I/O error on dev`,
}

type config struct {
	broker      string
	username    string
	password    string
	topicPrefix string
	cooldown    time.Duration
	source      string // "kmsg" or "stdin"
	dryRun      bool
	patternFile string
	discovery   bool
	clientID    string
}

type alertPayload struct {
	Host            string   `json:"host"`
	Timestamp       string   `json:"ts"`
	Severity        string   `json:"severity"` // "alert" or "summary"
	Summary         string   `json:"summary"`
	Pattern         string   `json:"pattern,omitempty"`
	Trigger         string   `json:"trigger,omitempty"`
	Context         []string `json:"context,omitempty"`
	SuppressedCount int      `json:"suppressed_count,omitempty"`
}

type publisher interface {
	publish(topic string, payload []byte, retain bool)
}

type dryRunPublisher struct{}

func (dryRunPublisher) publish(topic string, payload []byte, retain bool) {
	fmt.Printf("--- publish topic=%s retain=%v ---\n%s\n", topic, retain, payload)
}

type mqttPublisher struct{ client mqtt.Client }

func (p mqttPublisher) publish(topic string, payload []byte, retain bool) {
	tok := p.client.Publish(topic, 1, retain, payload)
	go func() {
		tok.Wait()
		if err := tok.Error(); err != nil {
			log.Printf("mqtt publish to %s failed: %v", topic, err)
		}
	}()
}

func main() {
	cfg := parseFlags()

	patterns, err := loadPatterns(cfg.patternFile)
	if err != nil {
		log.Fatalf("loading patterns: %v", err)
	}

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}

	lines := make(chan string, 4096)
	go func() {
		var err error
		switch cfg.source {
		case "stdin":
			err = readStdin(lines)
		default:
			err = readKmsg(lines)
		}
		if err != nil {
			log.Fatalf("log source failed: %v", err)
		}
		close(lines)
	}()

	var pub publisher
	if cfg.dryRun {
		pub = dryRunPublisher{}
	} else {
		pub = connectMQTT(cfg, hostname)
	}
	if cfg.discovery {
		publishDiscovery(pub, cfg, hostname)
	}

	run(cfg, pub, patterns, hostname, lines)

	// Source closed (stdin EOF in test mode): flush pending state and exit.
	log.Print("log source closed, exiting")
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.broker, "broker", envOr("MQTT_BROKER", "tcp://192.168.1.31:1883"), "MQTT broker URL")
	flag.StringVar(&cfg.username, "username", os.Getenv("MQTT_USERNAME"), "MQTT username")
	flag.StringVar(&cfg.password, "password", os.Getenv("MQTT_PASSWORD"), "MQTT password (prefer MQTT_PASSWORD env var)")
	flag.StringVar(&cfg.topicPrefix, "topic-prefix", envOr("TOPIC_PREFIX", "nas/dmesg-watch"), "MQTT topic prefix")
	flag.DurationVar(&cfg.cooldown, "cooldown", envDurationOr("COOLDOWN", 5*time.Minute), "suppression window between alerts; matches during it are aggregated into one summary")
	flag.StringVar(&cfg.source, "source", "kmsg", "log source: kmsg or stdin (stdin is for testing)")
	flag.BoolVar(&cfg.dryRun, "dry-run", false, "print alerts to stdout instead of publishing to MQTT")
	flag.StringVar(&cfg.patternFile, "patterns", os.Getenv("PATTERN_FILE"), "optional file with one regex per line, replacing built-in patterns")
	flag.BoolVar(&cfg.discovery, "discovery", true, "publish Home Assistant MQTT autodiscovery config")
	flag.StringVar(&cfg.clientID, "client-id", envOr("MQTT_CLIENT_ID", "nas-dmesg-watcher"), "MQTT client id")
	flag.Parse()
	return cfg
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDurationOr(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		log.Printf("invalid duration in %s=%q, using default %s", key, os.Getenv(key), def)
	}
	return def
}

func loadPatterns(path string) ([]*regexp.Regexp, error) {
	raw := defaultPatterns
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		raw = nil
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			raw = append(raw, line)
		}
	}
	var out []*regexp.Regexp
	for _, p := range raw {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("pattern %q: %w", p, err)
		}
		out = append(out, re)
	}
	if len(out) == 0 {
		return nil, errors.New("no patterns configured")
	}
	return out, nil
}

// readKmsg streams kernel log messages from /dev/kmsg, starting at the end of
// the buffer so old messages do not fire alerts on startup.
func readKmsg(lines chan<- string) error {
	f, err := os.Open("/dev/kmsg")
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek /dev/kmsg: %w", err)
	}
	buf := make([]byte, 16384)
	for {
		n, err := f.Read(buf)
		if err != nil {
			// EPIPE means the reader fell behind and records were
			// overwritten; skip the gap and keep reading.
			if errors.Is(err, syscall.EPIPE) {
				continue
			}
			return err
		}
		if msg, ok := parseKmsgRecord(string(buf[:n])); ok {
			lines <- msg
		}
	}
}

// parseKmsgRecord extracts the message text from a /dev/kmsg record of the
// form "pri,seq,timestamp,flags;message\n METADATA=...".
func parseKmsgRecord(rec string) (string, bool) {
	i := strings.IndexByte(rec, ';')
	if i < 0 {
		return "", false
	}
	msg := rec[i+1:]
	if j := strings.IndexByte(msg, '\n'); j >= 0 {
		msg = msg[:j]
	}
	return msg, msg != ""
}

func readStdin(lines chan<- string) error {
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		lines <- sc.Text()
	}
	return sc.Err()
}

func connectMQTT(cfg config, hostname string) publisher {
	statusTopic := cfg.topicPrefix + "/status"
	opts := mqtt.NewClientOptions().
		AddBroker(cfg.broker).
		SetClientID(cfg.clientID + "-" + hostname).
		SetUsername(cfg.username).
		SetPassword(cfg.password).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		SetWill(statusTopic, "offline", 1, true).
		SetOnConnectHandler(func(c mqtt.Client) {
			log.Print("connected to MQTT broker")
			c.Publish(statusTopic, 1, true, "online")
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			log.Printf("MQTT connection lost: %v", err)
		})
	client := mqtt.NewClient(opts)
	if tok := client.Connect(); tok.Wait() && tok.Error() != nil {
		// With SetConnectRetry the initial connect keeps retrying in the
		// background; a hard error here means the options are invalid.
		log.Fatalf("mqtt connect: %v", tok.Error())
	}
	return mqttPublisher{client: client}
}

// publishDiscovery announces a sensor to Home Assistant via MQTT discovery.
func publishDiscovery(pub publisher, cfg config, hostname string) {
	object := "dmesg_watch_" + sanitizeID(hostname)
	conf := map[string]any{
		"name":                  "Dmesg storage alert",
		"unique_id":             object,
		"state_topic":           cfg.topicPrefix + "/alert",
		"value_template":        "{{ value_json.summary }}",
		"json_attributes_topic": cfg.topicPrefix + "/alert",
		"availability_topic":    cfg.topicPrefix + "/status",
		"icon":                  "mdi:harddisk-remove",
		"device": map[string]any{
			"identifiers":  []string{"nas-dmesg-watcher-" + hostname},
			"name":         "dmesg watcher (" + hostname + ")",
			"manufacturer": "nas-dmesg-watcher",
		},
	}
	payload, _ := json.Marshal(conf)
	pub.publish("homeassistant/sensor/"+object+"/config", payload, true)
}

func sanitizeID(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, s)
}

// run is the main event loop: it maintains the before-context ring buffer,
// collects after-context for a triggered event, and enforces the cooldown
// window during which further matches are aggregated into a summary.
func run(cfg config, pub publisher, patterns []*regexp.Regexp, hostname string, lines <-chan string) {
	alertTopic := cfg.topicPrefix + "/alert"

	var before []string // ring buffer of last contextBefore lines

	var collecting *alertPayload
	collectTarget := 0
	var collectTimer *time.Timer
	collectDone := make(chan struct{}, 1)

	inCooldown := false
	suppressed := 0
	lastSuppressedLine := ""
	var cooldownTimer *time.Timer
	cooldownDone := make(chan struct{}, 1)

	match := func(line string) *regexp.Regexp {
		for _, re := range patterns {
			if re.MatchString(line) {
				return re
			}
		}
		return nil
	}

	finishEvent := func() {
		if collecting == nil {
			return
		}
		payload, _ := json.Marshal(collecting)
		pub.publish(alertTopic, payload, false)
		log.Printf("alert published: %s", collecting.Summary)
		collecting = nil
		if collectTimer != nil {
			collectTimer.Stop()
		}
		inCooldown = true
		if cooldownTimer != nil {
			cooldownTimer.Stop()
		}
		cooldownTimer = time.AfterFunc(cfg.cooldown, func() { cooldownDone <- struct{}{} })
	}

	cooldownExpired := func() {
		if suppressed > 0 {
			p := alertPayload{
				Host:            hostname,
				Timestamp:       time.Now().Format(time.RFC3339),
				Severity:        "summary",
				Summary:         truncate(fmt.Sprintf("+%d matches in last %s, last: %s", suppressed, cfg.cooldown, lastSuppressedLine), maxSummaryLen),
				Trigger:         lastSuppressedLine,
				SuppressedCount: suppressed,
			}
			payload, _ := json.Marshal(p)
			pub.publish(alertTopic, payload, false)
			log.Printf("summary published: %s", p.Summary)
			suppressed = 0
			lastSuppressedLine = ""
			// Storm still ongoing: keep summarizing once per window.
			cooldownTimer = time.AfterFunc(cfg.cooldown, func() { cooldownDone <- struct{}{} })
		} else {
			inCooldown = false
		}
	}

	for {
		select {
		case line, ok := <-lines:
			if !ok {
				finishEvent()
				cooldownExpired() // flush pending summary (test/stdin mode)
				return
			}
			if collecting != nil {
				collecting.Context = append(collecting.Context, line)
				if len(collecting.Context) >= collectTarget {
					finishEvent()
				}
			} else if re := match(line); re != nil {
				if inCooldown {
					suppressed++
					lastSuppressedLine = line
				} else {
					ctx := append(append([]string{}, before...), line)
					collecting = &alertPayload{
						Host:      hostname,
						Timestamp: time.Now().Format(time.RFC3339),
						Severity:  "alert",
						Summary:   truncate(line, maxSummaryLen),
						Pattern:   re.String(),
						Trigger:   line,
						Context:   ctx,
					}
					collectTarget = len(ctx) + contextAfter
					collectTimer = time.AfterFunc(collectTimeout, func() { collectDone <- struct{}{} })
				}
			}
			before = append(before, line)
			if len(before) > contextBefore {
				before = before[1:]
			}
		case <-collectDone:
			finishEvent()
		case <-cooldownDone:
			cooldownExpired()
		}
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
