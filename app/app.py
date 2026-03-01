import os
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st


OUTPUT_DIR = "outputs"


def _safe_read_csv(path, parse_dates=None):
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path, parse_dates=parse_dates)
    except Exception:
        return pd.DataFrame()


def _safe_read_alerts(path):
    if not os.path.exists(path):
        return []
    alerts = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    import json

                    entry = json.loads(line)
                    alerts.append(entry)
                except Exception:
                    # Ignore malformed lines
                    continue
    except Exception:
        return []
    return alerts


def load_data():
    kpi_daily = _safe_read_csv(os.path.join(OUTPUT_DIR, "kpi_daily.csv"), parse_dates=["delivery_date"])  # type: ignore[arg-type]
    anomalies = _safe_read_csv(os.path.join(OUTPUT_DIR, "anomalies.csv"), parse_dates=["delivery_date"])  # type: ignore[arg-type]
    recommendations = _safe_read_csv(os.path.join(OUTPUT_DIR, "recommendations.csv"))
    alerts = _safe_read_alerts(os.path.join(OUTPUT_DIR, "alerts.log"))
    return kpi_daily, anomalies, recommendations, alerts


def render_executive_overview(kpi_daily: pd.DataFrame, anomalies: pd.DataFrame):
    st.subheader("A. Executive Overview")

    if kpi_daily.empty:
        st.info("No KPI data available. Run the batch pipeline first.")
        return

    kpi_daily = kpi_daily.sort_values("delivery_date")
    last_date = kpi_daily["delivery_date"].max()
    window_start = last_date - timedelta(days=6)
    recent = kpi_daily[kpi_daily["delivery_date"] >= window_start]

    if recent.empty:
        recent = kpi_daily.tail(7)

    otif = float(recent["otif"].mean() * 100)
    fill_rate = float(recent["fill_rate"].mean() * 100)
    late_pct = float(recent["late_percent"].mean() * 100)

    high_anomalies = 0
    if not anomalies.empty and "severity" in anomalies.columns:
        cutoff = last_date - timedelta(days=7)
        recent_anom = anomalies[anomalies["delivery_date"] >= cutoff]
        high_anomalies = int((recent_anom["severity"] == "HIGH").sum())

    # Simple traffic light logic
    if otif >= 95 and fill_rate >= 97 and late_pct <= 5 and high_anomalies <= 2:
        status = "GREEN"
        status_descr = "Network performing within target thresholds."
    elif otif >= 90 and fill_rate >= 94 and late_pct <= 10 and high_anomalies <= 5:
        status = "YELLOW"
        status_descr = "Some stress signals detected. Monitor closely."
    else:
        status = "RED"
        status_descr = "Service risk elevated. Requires active intervention."

    cols = st.columns(5)
    cols[0].metric("OTIF (7d avg)", f"{otif:.1f}%")
    cols[1].metric("Fill rate (7d avg)", f"{fill_rate:.1f}%")
    cols[2].metric("Late % (7d avg)", f"{late_pct:.1f}%")
    cols[3].metric("HIGH anomalies (7d)", high_anomalies)
    cols[4].metric("Status", status)

    st.caption(status_descr)


def render_kpi_monitoring(kpi_daily: pd.DataFrame):
    st.subheader("B. KPI Monitoring")

    if kpi_daily.empty:
        st.info("No KPI time series available.")
        return

    kpi_daily = kpi_daily.sort_values("delivery_date")

    st.write("Daily KPI view")
    st.dataframe(
        kpi_daily[[
            "delivery_date",
            "otif",
            "fill_rate",
            "late_percent",
        ]].rename(columns={
            "delivery_date": "Date",
            "otif": "OTIF",
            "fill_rate": "Fill rate",
            "late_percent": "Late %",
        })
    )

    st.write("KPI trends")
    chart_df = kpi_daily[["delivery_date", "otif", "fill_rate", "late_percent"]].set_index("delivery_date")
    chart_df = chart_df * 100
    st.line_chart(chart_df)


def render_anomaly_monitoring(anomalies: pd.DataFrame):
    st.subheader("C. Anomaly Monitoring")

    if anomalies.empty:
        st.info("No anomalies detected or anomaly file not available.")
        return

    severity_options = ["ALL"] + sorted([s for s in anomalies["severity"].dropna().unique()])
    selected_severity = st.selectbox("Severity filter", severity_options, index=0)

    df = anomalies.copy()
    df = df.sort_values("delivery_date", ascending=False)
    if selected_severity != "ALL":
        df = df[df["severity"] == selected_severity]

    st.write("Recent anomalies")
    st.dataframe(
        df[[
            "delivery_date",
            "kpi_name",
            "actual_value",
            "rolling_mean",
            "z_score",
            "severity",
        ]].rename(columns={
            "delivery_date": "Date",
            "kpi_name": "KPI",
            "actual_value": "Actual",
            "rolling_mean": "Baseline",
            "z_score": "z-score",
        })
    )


def render_decision_support(recommendations: pd.DataFrame):
    st.subheader("D. Decision Support")

    if recommendations.empty:
        st.info("No recommended actions available yet. Run the pipeline or enable action logic.")
        return

    df = recommendations.copy()

    cols = st.columns(3)
    region_values = sorted(df["region"].dropna().unique()) if "region" in df.columns else []
    carrier_values = sorted(df["carrier"].dropna().unique()) if "carrier" in df.columns else []
    priority_values = sorted(df["priority"].dropna().unique()) if "priority" in df.columns else []

    region_filter = cols[0].multiselect("Region", region_values, default=region_values)
    carrier_filter = cols[1].multiselect("Carrier", carrier_values, default=carrier_values)
    priority_filter = cols[2].multiselect("Priority", priority_values, default=priority_values)

    if region_filter and "region" in df.columns:
        df = df[df["region"].isin(region_filter)]
    if carrier_filter and "carrier" in df.columns:
        df = df[df["carrier"].isin(carrier_filter)]
    if priority_filter and "priority" in df.columns:
        df = df[df["priority"].isin(priority_filter)]

    st.write("Recommended actions")
    st.dataframe(df)


def render_scenario_planning(kpi_daily: pd.DataFrame):
    st.subheader("E. Scenario Planning")

    if kpi_daily.empty:
        st.info("No KPI baseline available for scenario planning.")
        return

    kpi_daily = kpi_daily.sort_values("delivery_date")
    recent = kpi_daily.tail(30)

    baseline_otif = float(recent["otif"].mean() * 100)
    baseline_fill = float(recent["fill_rate"].mean() * 100)
    baseline_late = float(recent["late_percent"].mean() * 100)

    st.write("Baseline (last 30 days)")
    cols = st.columns(3)
    cols[0].metric("OTIF", f"{baseline_otif:.1f}%")
    cols[1].metric("Fill rate", f"{baseline_fill:.1f}%")
    cols[2].metric("Late %", f"{baseline_late:.1f}%")

    st.markdown("---")
    st.write("Directional scenario (illustrative only)")

    vol_change = st.slider("Volume change vs baseline", -20, 20, 0, help="Approximate change in total order volume.")
    carrier_reliability = st.slider("Carrier reliability", -10, 10, 0, help="Relative change in carrier performance.")
    lead_time_buffer = st.slider("Lead time buffer (days)", 0, 5, 1, help="Operational buffer added to planned lead time.")

    # Simple directional adjustments (illustrative, not a forecast)
    scenario_otif = baseline_otif + carrier_reliability * 0.3 - max(vol_change, 0) * 0.2
    scenario_fill = baseline_fill - max(vol_change, 0) * 0.2 + lead_time_buffer * 0.3
    scenario_late = baseline_late + max(vol_change, 0) * 0.3 - lead_time_buffer * 0.8

    scenario_otif = max(0.0, min(100.0, scenario_otif))
    scenario_fill = max(0.0, min(100.0, scenario_fill))
    scenario_late = max(0.0, min(100.0, scenario_late))

    cols2 = st.columns(3)
    cols2[0].metric("Scenario OTIF", f"{scenario_otif:.1f}%", f"{scenario_otif - baseline_otif:+.1f} pp")
    cols2[1].metric("Scenario Fill", f"{scenario_fill:.1f}%", f"{scenario_fill - baseline_fill:+.1f} pp")
    cols2[2].metric("Scenario Late %", f"{scenario_late:.1f}%", f"{scenario_late - baseline_late:+.1f} pp")

    st.caption("Scenario logic is directional and illustrative only — not a statistical forecast.")


def render_alerts_and_reporting(alerts, report_path: str):
    st.subheader("F. Alerts & Reporting")

    if not alerts:
        st.info("No alerts recorded yet.")
    else:
        st.write("Recent alerts")
        rows = []
        for entry in alerts[-50:]:
            ts = entry.get("timestamp")
            alert = entry.get("alert", {})
            rows.append(
                {
                    "timestamp": ts,
                    "type": alert.get("type"),
                    "severity": alert.get("severity"),
                    "message": alert.get("message"),
                }
            )
        df = pd.DataFrame(rows)
        st.dataframe(df.sort_values("timestamp", ascending=False))

    if os.path.exists(report_path):
        st.markdown("---")
        st.write("HTML report")
        st.markdown(
            f"[Open latest HTML report]({report_path})",
            help="Opens the static HTML report generated by the batch pipeline.",
        )
    else:
        st.info("HTML report not found. Run the batch pipeline to generate it.")


def main():
    st.set_page_config(
        page_title="Supply Chain Control Tower",
        layout="wide",
    )

    st.title("Supply Chain Control Tower")
    st.caption("Internal operations cockpit – powered by batch pipeline outputs.")

    kpi_daily, anomalies, recommendations, alerts = load_data()
    report_path = os.path.join(OUTPUT_DIR, "report_latest.html")

    render_executive_overview(kpi_daily, anomalies)
    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        render_kpi_monitoring(kpi_daily)
    with col2:
        render_anomaly_monitoring(anomalies)

    st.markdown("---")
    render_decision_support(recommendations)

    st.markdown("---")
    render_scenario_planning(kpi_daily)

    st.markdown("---")
    render_alerts_and_reporting(alerts, report_path)


if __name__ == "__main__":
    main()
