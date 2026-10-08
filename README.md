# nas-dmesg-watcher

Mały daemon w Go, który czyta `/dev/kmsg` (strumień dmesg) i gdy wykryje błędy
podsystemu dyskowego (bcachefs, libata, SCSI, I/O errors), publikuje alert z
kontekstem (3 linie przed, 10 po) na MQTT. Home Assistant wykrywa sensor
automatycznie (MQTT autodiscovery) i może wysłać powiadomienie push.

## Mechanizm antyspamowy

Burza błędów (setki linii/s podczas awarii kontrolera) nie zaleje HA:

- pierwszy błąd → **natychmiastowy alert** z pełnym kontekstem,
- kolejne trafienia w oknie `COOLDOWN` (domyślnie 5 min) są zliczane,
- po upływie okna wychodzi jedno **podsumowanie** („+897 matches…"); dopóki
  burza trwa, podsumowanie powtarza się raz na okno.

## Budowanie

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o dist/nas-dmesg-watcher
```

## Instalacja (Proxmox / dowolny systemd)

```bash
scp dist/nas-dmesg-watcher root@nas:/usr/local/bin/nas-dmesg-watcher
scp deploy/nas-dmesg-watcher.service root@nas:/etc/systemd/system/
scp deploy/nas-dmesg-watcher.env.example root@nas:/etc/nas-dmesg-watcher.env
ssh root@nas 'chmod 600 /etc/nas-dmesg-watcher.env'   # uzupełnij login/hasło MQTT
ssh root@nas 'systemctl daemon-reload && systemctl enable --now nas-dmesg-watcher'
```

Weryfikacja: `journalctl -u nas-dmesg-watcher -f` powinno pokazać
`connected to MQTT broker`, a w HA pojawi się urządzenie **dmesg watcher (ugreen)**
z sensorem **Dmesg storage alert**.

## Home Assistant

Sensor wystawia się sam przez autodiscovery. Automatyzację (persistent
notification + push na telefon) weź z
[deploy/home-assistant-automation.yaml](deploy/home-assistant-automation.yaml) —
podmień `notify.mobile_app_CHANGE_ME` na swoją usługę notify.

## Topici i payload

| Topic | Treść |
|---|---|
| `nas/dmesg-watch/alert` | JSON alertu (QoS 1, bez retain) |
| `nas/dmesg-watch/status` | `online`/`offline` (retained, LWT) |
| `homeassistant/sensor/.../config` | autodiscovery (retained) |

Payload alertu: `host`, `ts`, `severity` (`alert`/`summary`), `summary`,
`pattern`, `trigger`, `context[]`, `suppressed_count`.

## Konfiguracja

Zmienne środowiskowe (albo flagi — `-h`): `MQTT_BROKER`, `MQTT_USERNAME`,
`MQTT_PASSWORD`, `TOPIC_PREFIX`, `COOLDOWN`, `PATTERN_FILE` (własne regexy,
jeden na linię, zastępują wbudowane).

Wbudowane wzorce łapią: błędy bcachefs, wyjątki/resety/disable libata
(`ata*: exception/interface fatal/failed to IDENTIFY/disable device/...`),
`DID_*` z warstwy SCSI oraz `I/O error, dev ...`.

## Test lokalny

```bash
go run . -source stdin -dry-run -cooldown 2s < przykładowy-dmesg.txt
```
