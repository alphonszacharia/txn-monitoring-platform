"""Streamlit dashboard for the transaction monitoring platform.

Run from the project root:
    streamlit run dashboard/app.py
"""
import os
import sys
import time
from pathlib import Path

# `streamlit run dashboard/app.py` puts only dashboard/ on the path, so add the
# project root to let `from dashboard import ...` resolve however it is launched.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import altair as alt  # noqa: E402
import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from dashboard import queries  # noqa: E402

st.set_page_config(page_title="Transaction Monitoring", page_icon="🏦", layout="wide")


@st.cache_data(ttl=60)
def load(name, **params):
    return queries.run(getattr(queries, name), params or None)


RULE_LABELS = {
    "structuring": "Structuring",
    "rapid_in_out": "Rapid in/out",
    "high_risk_country": "High-risk country",
}

st.title("Transaction Monitoring")
st.caption("Synthetic banking data, loaded and modelled by the pipeline. "
           "No real customer data is used.")

try:
    overview = load("OVERVIEW").iloc[0]
    alerts = load("ALERTS")
    evaluation = load("EVALUATION")
    daily = load("DAILY_VOLUME")
except Exception as exc:  # database down, or the pipeline has not finished yet
    # On a first `docker compose up` the data is still being loaded, so wait and retry.
    st.info("The warehouse is not ready yet. If this is a first start, the pipeline "
            "is still loading data (a few minutes). This page retries automatically.")
    with st.expander("Technical detail"):
        st.code(str(exc))
    time.sleep(float(os.getenv("DASHBOARD_RETRY_SECONDS", "10")))
    st.rerun()

if alerts.empty:
    st.warning("The alerts table is empty. Run the pipeline first.")
    st.stop()

alerts["rule"] = alerts["rule_name"].map(RULE_LABELS)
alerts["alert_date"] = pd.to_datetime(alerts["alert_date"])
alerts["total_amount"] = alerts["total_amount"].astype(float)

# ------------------------------------------------------------------ sidebar
st.sidebar.header("Filters")
rules = st.sidebar.multiselect("Rule", list(RULE_LABELS.values()),
                               default=list(RULE_LABELS.values()))
first, last = alerts["alert_date"].min().date(), alerts["alert_date"].max().date()
date_range = st.sidebar.date_input("Alert date", value=(first, last),
                                   min_value=first, max_value=last)
kyc = st.sidebar.multiselect("Customer KYC risk", ["low", "medium", "high"],
                             default=["low", "medium", "high"])
only_fp = st.sidebar.checkbox("Only false positives (uses synthetic ground truth)")

start, end = (date_range if len(date_range) == 2 else (first, last))
view = alerts[
    alerts["rule"].isin(rules)
    & alerts["alert_date"].between(pd.Timestamp(start), pd.Timestamp(end))
    & alerts["kyc_risk_rating"].isin(kyc)
]
if only_fp:
    view = view[view["outcome"] == "false positive"]

# --------------------------------------------------------------------- KPIs
k1, k2, k3, k4 = st.columns(4)
k1.metric("Transactions", f"{int(overview['transactions']):,}")
k2.metric("Volume", f"€{float(overview['volume_eur']):,.0f}")
k3.metric("Alerts (filtered)", f"{len(view):,}")
k4.metric("Accounts alerted", f"{view['account_id'].nunique():,}")

tab_alerts, tab_rules, tab_drill = st.tabs(["Alerts", "Rule performance", "Investigate"])

# ------------------------------------------------------------------- alerts
with tab_alerts:
    if view.empty:
        st.info("No alerts match the current filters.")
    else:
        per_day = (view.groupby([view["alert_date"].dt.date.rename("day"), "rule"])
                   .size().reset_index(name="alerts"))
        chart = (alt.Chart(per_day).mark_bar()
                 .encode(x=alt.X("day:T", title="Day"),
                         y=alt.Y("alerts:Q", title="Alerts"),
                         color=alt.Color("rule:N", title="Rule"),
                         tooltip=["day:T", "rule:N", "alerts:Q"])
                 .properties(height=280))
        st.altair_chart(chart, width="stretch")

        st.dataframe(
            view[["alert_date", "rule", "account_id", "full_name", "country_code",
                  "kyc_risk_rating", "account_type", "txn_count", "total_amount",
                  "outcome"]].rename(columns={
                      "alert_date": "Date", "rule": "Rule", "account_id": "Account",
                      "full_name": "Customer", "country_code": "Country",
                      "kyc_risk_rating": "KYC risk", "account_type": "Account type",
                      "txn_count": "Txns", "total_amount": "Total (€)",
                      "outcome": "Outcome"}),
            hide_index=True, width="stretch",
            column_config={
                "Date": st.column_config.DateColumn(format="YYYY-MM-DD"),
                "Total (€)": st.column_config.NumberColumn(format="%.2f"),
            },
        )
        st.caption("Outcome compares each alert with the synthetic ground truth. "
                   "'High-risk country' is a screening rule with no planted answers.")

# --------------------------------------------------------------------- rules
with tab_rules:
    st.subheader("Precision and recall against planted patterns")
    if evaluation.empty:
        st.info("No evaluation data. Load the ground truth and run dbt.")
    else:
        ev = evaluation.copy()
        ev["rule"] = ev["rule_name"].map(RULE_LABELS)
        cols = st.columns(len(ev))
        for col, (_, row) in zip(cols, ev.iterrows(), strict=True):
            col.markdown(f"**{row['rule']}**")
            col.metric("Precision", f"{float(row['precision']):.3f}")
            col.metric("Recall", f"{float(row['recall']):.3f}")
            col.caption(f"{int(row['true_positives'])} true positives · "
                        f"{int(row['false_positives'])} false positives · "
                        f"{int(row['false_negatives'])} missed")
    st.markdown(
        "Precision is the share of alerts that were real. Recall is the share of "
        "real cases that were caught. The generator plants legitimate look-alikes, "
        "so the rules can be wrong. The refinements in `dbt_project.yml` trade a "
        "little recall risk for far fewer false alarms, and the dbt quality gate "
        "fails the build if either figure drops below its threshold."
    )

# --------------------------------------------------------------- investigate
with tab_drill:
    if view.empty:
        st.info("No alerts match the current filters.")
    else:
        options = view.assign(
            label=view["alert_date"].dt.strftime("%Y-%m-%d") + " · " + view["rule"]
            + " · " + view["account_id"] + " · " + view["full_name"]
        ).head(200)
        choice = st.selectbox("Pick an alert (latest 200 shown)", options["label"])
        picked = options[options["label"] == choice].iloc[0]
        st.write(f"**{picked['full_name']}** ({picked['country_code']}, "
                 f"KYC {picked['kyc_risk_rating']}, {picked['account_type']} account) "
                 f"· outcome: **{picked['outcome']}**")
        activity = load("ACCOUNT_ACTIVITY", account_id=picked["account_id"],
                        day=picked["alert_date"].date())
        st.caption("All activity on the account the day before, the day of and the "
                   "day after the alert.")
        st.dataframe(activity, hide_index=True, width="stretch")

with st.expander("Volume by day"):
    st.bar_chart(daily.set_index("txn_date")["transactions"])
