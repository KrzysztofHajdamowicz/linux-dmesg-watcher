# nas-dmesg-watcher

> Dokumentacja po polsku: [README.PL.md](README.PL.md)

A small Go daemon that reads `/dev/kmsg` (the dmesg stream) and, when it
detects storage-subsystem errors (bcachefs, libata, SCSI, I/O errors),
publishes an alert with context (3 lines before, 10 after) to MQTT. Home
Assistant picks the sensor up automatically (MQTT autodiscovery) and can send
a push notification.

## Alert-storm suppression

An error storm (hundreds of lines per second during a controller failure)
won't flood HA:

- the first error → an **immediate alert** with full context,
- further matches within the `COOLDOWN` window (default 5 min) are counted,
- when the window expires, a single **summary** goes out ("+897 matches…");
  as long as the storm lasts, the summary repeats once per window.

## Building

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o dist/nas-dmesg-watcher
```

## Installation (Proxmox / any systemd distro)

```bash
scp dist/nas-dmesg-watcher root@nas:/usr/local/bin/nas-dmesg-watcher
scp deploy/nas-dmesg-watcher.service root@nas:/etc/systemd/system/
scp deploy/nas-dmesg-watcher.env.example root@nas:/etc/nas-dmesg-watcher.env
ssh root@nas 'chmod 600 /etc/nas-dmesg-watcher.env'   # fill in MQTT credentials
ssh root@nas 'systemctl daemon-reload && systemctl enable --now nas-dmesg-watcher'
```

Verify: `journalctl -u nas-dmesg-watcher -f` should show
`connected to MQTT broker`, and a **dmesg watcher (<hostname>)** device with a
**Dmesg storage alert** sensor appears in HA.

## Home Assistant

The sensor registers itself via autodiscovery. Take the automation
(persistent notification + mobile push) from
[deploy/home-assistant-automation.yaml](deploy/home-assistant-automation.yaml)
and replace `notify.mobile_app_CHANGE_ME` with your notify service.

## Topics and payload

| Topic | Content |
|---|---|
| `nas/dmesg-watch/alert` | alert JSON (QoS 1, not retained) |
| `nas/dmesg-watch/status` | `online`/`offline` (retained, LWT) |
| `homeassistant/sensor/.../config` | autodiscovery (retained) |

Alert payload fields: `host`, `ts`, `severity` (`alert`/`summary`), `summary`,
`pattern`, `trigger`, `context[]`, `suppressed_count`.

## Configuration

Environment variables (or flags — see `-h`): `MQTT_BROKER`, `MQTT_USERNAME`,
`MQTT_PASSWORD`, `TOPIC_PREFIX`, `COOLDOWN`, `PATTERN_FILE` (custom regexes,
one per line, replacing the built-in ones).

Built-in patterns match: bcachefs errors, libata exceptions/resets/disables
(`ata*: exception/interface fatal/failed to IDENTIFY/disable device/...`),
SCSI-layer `DID_*` failures, and `I/O error, dev ...`.

## Local testing

```bash
go run . -source stdin -dry-run -cooldown 2s < sample-dmesg.txt
```
