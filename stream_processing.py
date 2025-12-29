

"""Stream processing (Wikimedia EventStreams)
- Listen to Wikipedia edit events in real time (SSE stream)
- Track 5 entities (Wikipedia pages)
- Compute simple metrics (edits count, last edit time)
- Store metrics to CSV + snapshot JSON
- Generate alerts when edits exceed a threshold in a time window

How to run:
  pip install requests
  python stream_processing.py

Outputs:
  stream_output/metrics.csv
  stream_output/metrics_snapshot.json
  stream_output/alerts.log
"""

import os
import json
import csv
import time
from datetime import datetime, timezone
from collections import deque, defaultdict

import requests


TRACKED_TITLES = {
    "United States",
    "Donald Trump",
    "Elon Musk",
    "YouTube",
    "Wikipedia",
}

# Stream endpoint (Wikimedia recent changes)
EVENTS_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# Output folders/files
OUT_DIR = "stream_output"
METRICS_CSV = os.path.join(OUT_DIR, "metrics.csv")
SNAPSHOT_JSON = os.path.join(OUT_DIR, "metrics_snapshot.json")
ALERTS_LOG = os.path.join(OUT_DIR, "alerts.log")

# Alert rule
ALERT_WINDOW_SECONDS = 60 * 30  # 30 minutes
ALERT_COUNT = 5

# How often to flush metrics to disk
FLUSH_EVERY_SECONDS = 20


# HELPERS


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_out_dir() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)


def write_csv_header_if_needed(path: str) -> None:
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "timestamp_utc",
                    "title",
                    "total_edits",
                    "last_edit_utc",
                    "last_user",
                    "last_comment",
                ]
            )


def append_metrics_row(title: str, m: dict) -> None:
    with open(METRICS_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                iso_now(),
                title,
                m["total_edits"],
                m["last_edit_utc"] or "",
                m["last_user"] or "",
                (m["last_comment"] or "")[:200],
            ]
        )


def write_snapshot(all_metrics: dict) -> None:
    with open(SNAPSHOT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_metrics, f, ensure_ascii=False, indent=2)


def log_alert(msg: str) -> None:
    line = f"[{iso_now()}] {msg}\n"
    with open(ALERTS_LOG, "a", encoding="utf-8") as f:
        f.write(line)



# MAIN STREAM LOOP


def run_stream() -> None:
    ensure_out_dir()
    write_csv_header_if_needed(METRICS_CSV)

    # metrics per title
    metrics = {
        title: {
            "total_edits": 0,
            "last_edit_utc": None,
            "last_user": None,
            "last_comment": None,
        }
        for title in TRACKED_TITLES
    }

    # for alerting: store edit timestamps per title in a sliding window
    recent_edits = defaultdict(lambda: deque())  # title -> deque[timestamps_epoch]

    last_flush = time.time()
    # Force an initial snapshot so a JSON file exists even if no events occur
    write_snapshot(metrics)

    # SSE stream: requests with stream=True
    headers = {
        "Accept": "text/event-stream",
        "User-Agent": "ECE-BigData-Student/1.0 (contact: student@ece.fr)"
    }

    while True:
        try:
            with requests.get(EVENTS_URL, headers=headers, stream=True, timeout=60) as r:
                r.raise_for_status()

                for raw_line in r.iter_lines(decode_unicode=True):
                    if not raw_line:
                        continue

                    # SSE messages usually contain lines like: "data: {...}"
                    if not raw_line.startswith("data:"):
                        continue

                    data_str = raw_line[len("data:") :].strip()
                    if data_str == "[DONE]":
                        continue

                    try:
                        event = json.loads(data_str)
                    except json.JSONDecodeError:
                        continue

                    # Event example fields: title, user, comment, timestamp, type
                    title = event.get("title")
                    if title not in TRACKED_TITLES:
                        continue

                    user = event.get("user")
                    comment = event.get("comment")
                    ts = event.get("timestamp")  # epoch seconds
                    if ts is None:
                        ts = int(time.time())

                    # Update metrics
                    metrics[title]["total_edits"] += 1
                    metrics[title]["last_edit_utc"] = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                    metrics[title]["last_user"] = user
                    metrics[title]["last_comment"] = comment

                    # Update alert window
                    dq = recent_edits[title]
                    dq.append(ts)
                    cutoff = ts - ALERT_WINDOW_SECONDS
                    while dq and dq[0] < cutoff:
                        dq.popleft()

                    # Trigger alert if threshold met
                    if len(dq) >= ALERT_COUNT:
                        log_alert(
                            f"ALERT: '{title}' had {len(dq)} edits in the last {ALERT_WINDOW_SECONDS // 60} minutes."
                        )
                        # Clear to avoid repeated spam alerts
                        dq.clear()

                    # Flush periodically
                    now = time.time()
                    if now - last_flush >= FLUSH_EVERY_SECONDS:
                        for t in TRACKED_TITLES:
                            append_metrics_row(t, metrics[t])
                        write_snapshot(metrics)
                        last_flush = now

        except (requests.RequestException, TimeoutError) as e:
            # Network hiccup: wait and reconnect
            log_alert(f"INFO: reconnecting after error: {repr(e)}")
            time.sleep(5)
        except KeyboardInterrupt:
            # final snapshot on exit
            write_snapshot(metrics)
            print("Stopped. Snapshot saved.")
            break


if __name__ == "__main__":
    print("Tracking titles:")
    for t in sorted(TRACKED_TITLES):
        print(" -", t)
    print(f"Alert rule: {ALERT_COUNT} edits / {ALERT_WINDOW_SECONDS // 60} min")
    print("Output folder:", OUT_DIR)
    run_stream()
