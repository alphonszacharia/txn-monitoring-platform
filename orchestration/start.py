"""Entrypoint of the `dagster` container: one command brings everything up.

    python -m orchestration.start

1. Starts the Dagster UI and daemon (`dagster dev`).
2. On the first start, runs the whole pipeline through Dagster (reference data,
   7 daily runs, dbt build and tests). If the warehouse is already built, as on a
   later `docker compose up`, this step is skipped.
3. Writes /tmp/pipeline_status ("ok" or "failed"). The compose healthcheck waits
   for that file, so the dashboard container starts only once the data is ready
   (or the pipeline has failed, so nothing waits forever).
4. Keeps the UI running until the container is stopped.
"""
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

from ingestion import load_raw as lr
from orchestration import run_pipeline

STATUS_FILE = Path("/tmp/pipeline_status")
PORT = 3000
EXPECTED_DAYS = run_pipeline.DAYS


def ui_is_up():
    try:
        with urllib.request.urlopen(f"http://localhost:{PORT}/server_info", timeout=2):
            return True
    except Exception:
        return False


def warehouse_is_built():
    """True if all days are in raw and the dbt marts exist."""
    try:
        conn = lr.connect()
    except Exception:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("select to_regclass('marts.eval_alert_performance') is not null")
            if not cur.fetchone()[0]:
                return False
            cur.execute("select count(distinct _batch_id) from raw.transactions")
            return cur.fetchone()[0] >= EXPECTED_DAYS
    except Exception:
        return False
    finally:
        conn.close()


def main():
    STATUS_FILE.unlink(missing_ok=True)
    server = subprocess.Popen(
        ["dagster", "dev", "-m", "orchestration.definitions",
         "-h", "0.0.0.0", "-p", str(PORT)]
    )

    def shut_down(signum, frame):
        # Ask Dagster to stop; force it if it has not stopped within 8 seconds
        # (Docker itself force-stops the container after 10 seconds).
        server.terminate()
        threading.Timer(8, server.kill).start()

    signal.signal(signal.SIGTERM, shut_down)
    signal.signal(signal.SIGINT, shut_down)

    # Let Dagster finish creating its storage before the pipeline runs use it.
    deadline = time.time() + 300
    while time.time() < deadline and server.poll() is None and not ui_is_up():
        time.sleep(2)
    if server.poll() is not None:
        sys.exit(server.returncode)

    status = "ok"
    if warehouse_is_built():
        print("\nWarehouse already built: skipping the pipeline run.", flush=True)
    else:
        print("\nFirst start: running the pipeline through Dagster...", flush=True)
        try:
            run_pipeline.main()
        except SystemExit as exc:
            status = "failed"
            print(f"\nPIPELINE FAILED: {exc}. Open http://localhost:{PORT} for the "
                  "run logs.", flush=True)

    STATUS_FILE.write_text(status)
    if status == "ok":
        print(f"\nREADY. Dagster: http://localhost:{PORT}  "
              "Dashboard: http://localhost:8501", flush=True)
    return server.wait()


if __name__ == "__main__":
    sys.exit(main())
