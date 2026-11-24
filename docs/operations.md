# Operations: deploying on a NAS, a Raspberry Pi 5 and a server PC

The example configs assume this addressing (placeholders, adapt to your LAN):

| node | address | role notes |
|---|---|---|
| `nas` | 10.0.0.10 | bulk disk; data dir `/srv/raftstore` |
| `pi5` | 10.0.0.11 | put the data dir on an SSD/NVMe, not the SD card; slow scrub rate |
| `server` | 10.0.0.12 | fastest machine; will usually win elections first |

Ports: 7000/tcp (Raft, node↔node only), 8080/tcp (HTTP API).

## Install (each machine)

```sh
sudo useradd --system --home /var/lib/raftstore raftstore
sudo mkdir -p /opt/raftstore /etc/raftstore/tls /var/lib/raftstore
sudo cp -r raftstore /opt/raftstore/            # the Python package; stdlib only, Python >= 3.11
sudo cp deploy/config/<name>.json /etc/raftstore/node.json
sudo cp deploy/systemd/raftstore.service /etc/systemd/system/
sudo chown -R raftstore:raftstore /var/lib/raftstore /srv/raftstore 2>/dev/null || true
# TLS: see deploy/tls/README.md, then
sudo systemctl daemon-reload && sudo systemctl enable --now raftstore
```

Start all three; the first election takes ~1–2 s. Check:

```sh
export RAFTSTORE_URLS=https://10.0.0.10:8080,https://10.0.0.11:8080,https://10.0.0.12:8080
export RAFTSTORE_CACERT=/etc/raftstore/tls/ca.pem RAFTSTORE_CERT=~/client.pem RAFTSTORE_KEY=~/client.key
python3 -m raftstore status --all
python3 -m raftstore put backups/photos.tar ./photos.tar
python3 -m raftstore get backups/photos.tar -o /tmp/check.tar
```

Disk hygiene: on the NAS make sure the drive's volatile write cache is either protected (PLP/UPS) or
disabled (`hdparm -W 0 /dev/sdX`), otherwise `fsync` may lie. Prefer ext4/xfs with default `data=ordered`.

## Replacing a node whose drive died

The process exits with code **75** on a storage error (systemd will not restart-loop it). Then:

```sh
python3 -m raftstore member remove nas            # 3 -> 2 voters (still a majority)
# on the replacement machine/drive: deploy/config/nas2-replacement.json ("join": true)
sudo systemctl start raftstore
python3 -m raftstore member add nas2 --raft 10.0.0.13:7000 --http 10.0.0.13:8080
python3 -m raftstore status --all                 # nas2 catches up via snapshot + chunk repair
```

Remove first, then add: with one node down, adding first would create a 4-voter cluster that needs 3
live voters — including the brand-new, still-empty one — for every commit.

## Routine

* `raftstore scrub` (or the `raftstore-scrub.timer`) — on-demand integrity pass on one node.
* `raftstore snapshot` — force a snapshot + WAL compaction on the leader.
* `raftstore gc --grace 1000` — drop orphaned chunks (e.g. from interrupted uploads).
* `raftstore transfer <id>` — move leadership away before maintenance on the leader.

---

## Hardware validation / TODO for the author

**Nothing in this section has been run yet.** All numbers in the README were measured on a single
Windows 10 development machine with three local processes. The commands below are what should be run on
the real NAS / Pi 5 / server PC; fill in the tables with what you observe.

### 1. Bring-up

```sh
# on each machine, after the install steps above
sudo systemctl status raftstore
python3 -m raftstore status --all --json | jq '.state, .term, .leader, .commit'
```

| check | nas | pi5 | server |
|---|---|---|---|
| service active | | | |
| joined cluster (voters = 3) | | | |
| time to first leader (s) | | | |

### 2. Throughput / latency over the LAN

```sh
python3 scripts/bench.py --url https://10.0.0.10:8080,https://10.0.0.11:8080,https://10.0.0.12:8080
```

| phase | ops/s | MB/s | p50 (ms) | p99 (ms) |
|---|---|---|---|---|
| seq PUT 1 KiB | | | | |
| 8× concurrent PUT 1 KiB | | | | |
| seq PUT 1 MiB | | | | |
| linearizable GET 1 KiB | | | | |

### 3. Pull a drive mid-write (the headline test, for real)

1. Start a writer: `while true; do head -c 100000 /dev/urandom > /tmp/x; python3 -m raftstore put "w/$(date +%s%N)" /tmp/x && echo ok; done | tee acked.log`
   (better: run `python3 scripts/chaos_demo.py`-style writers from a fourth machine and record acked keys).
2. Physically unplug the data drive of the **current leader** (USB/SATA hot-plug on the NAS) while writes run.
3. Expect: the node logs `FAIL-STOP` and exits with 75; a new leader within ~2 s; writes continue.
4. Verify every key listed as acknowledged: `python3 -m raftstore get <key> -o /dev/null` for all of them.
5. Re-insert the drive and start the service (or replace the node as above) and check it converges.

| run | node pulled | detected after (ms) | new leader after (ms) | acked writes | acked writes missing | notes |
|---|---|---|---|---|---|---|
| 1 | | | | | | |
| 2 | | | | | | |
| 3 | | | | | | |

### 4. Power cut

Pull the power of the leader (and, separately, of all three machines at once) during writes.

| run | what lost power | acked writes | missing after restart | recovery notes in /status |
|---|---|---|---|---|
| 1 | | | | |
| 2 | | | | |

### 5. Bit rot on real disk

```sh
# on a follower: pick a chunk and flip one byte (as root)
f=$(find /var/lib/raftstore/chunks -type f -name '????????????????????????????????????????????????????????????????' | head -1)
printf '\x00' | dd of="$f" bs=1 seek=100 conv=notrunc
python3 -m raftstore --url https://<that-node>:8080 scrub
```

| node | chunks checked | corrupt found | repaired | scrub duration |
|---|---|---|---|---|
| | | | | |

### 6. Pi 5 specifics to check

* SD card vs NVMe data dir: fsync latency (`seq PUT 1 KiB` p50) on each.
* CPU cost of SHA-256 during scrub at the configured `scrub_rate_bytes` (8 MiB/s in `pi5.json`).
