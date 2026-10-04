"""Smoke test: the dashboard renders against a built warehouse without errors.

Skipped unless RUN_DASHBOARD_SMOKE=1, because it needs Postgres with the marts built
(CI sets it after `dbt build`).
"""
import os
from pathlib import Path

import pytest

pytest.importorskip("streamlit")

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DASHBOARD_SMOKE") != "1",
    reason="needs a built warehouse; set RUN_DASHBOARD_SMOKE=1",
)


def test_dashboard_renders():
    from streamlit.testing.v1 import AppTest

    app = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"
    at = AppTest.from_file(str(app), default_timeout=60).run()
    assert not at.exception, [e.value for e in at.exception]
    assert not at.error, [e.value for e in at.error]
    labels = [m.label for m in at.metric]
    assert "Transactions" in labels and "Precision" in labels
