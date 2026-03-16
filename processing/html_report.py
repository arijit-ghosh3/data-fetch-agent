from processing.transformer import (
    _assign_rows_to_stages, _compute_timings, _fmt_td,
)


def _td_to_seconds(td):
    return td.total_seconds() if td else 0


def _build_comparison_table(all_runs):
    """Build side-by-side comparison data for all runs."""
    all_labels = []
    for run in all_runs:
        for t in run["timings"]:
            if t["label"] not in all_labels:
                all_labels.append(t["label"])
    if "Total Processing Time" not in all_labels:
        all_labels.append("Total Processing Time")
    if any(r["case_count"] > 0 for r in all_runs):
        all_labels.append("Time per Case")

    rows = []
    for label in all_labels:
        row = {"label": label, "values": []}
        for run in all_runs:
            val = ""
            if label == "Total Processing Time":
                val = _fmt_td(run["total_time"])
            elif label == "Time per Case":
                if run["case_count"] > 0:
                    val = _fmt_td(run["total_time"] / run["case_count"])
            else:
                for t in run["timings"]:
                    if t["label"] == label:
                        val = _fmt_td(t["duration"])
                        break
            row["values"].append(val)
        rows.append(row)
    return rows


def _build_bar_chart_data(run):
    """Build data for a single run's stage breakdown chart."""
    chart_data = []
    for t in run["timings"]:
        secs = _td_to_seconds(t["duration"])
        chart_data.append({
            "label": t["label"],
            "seconds": secs,
            "formatted": _fmt_td(t["duration"]),
            "type": t["type"],
        })
    return chart_data


def generate_html_report(all_runs, output_path):
    guid_labels = []
    for r in all_runs:
        guid_short = r["job_guid"][:8]
        guid_labels.append(f"{guid_short}... ({r['case_count']} cases)")

    comparison_rows = _build_comparison_table(all_runs)

    # Colors for chart bars
    colors_stage = ["#4472C4", "#5B9BD5", "#2F5496", "#7FB3E0"]
    colors_wait = "#FFC000"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Performance Comparison Report</title>
<style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #f5f7fa; color: #333; padding: 20px; }}
    .container {{ max-width: 1400px; margin: 0 auto; }}
    h1 {{ color: #2F5496; margin-bottom: 8px; font-size: 28px; }}
    h2 {{ color: #4472C4; margin: 30px 0 15px 0; font-size: 20px; border-bottom: 2px solid #4472C4; padding-bottom: 6px; }}
    h3 {{ color: #2F5496; margin: 20px 0 10px 0; font-size: 16px; }}
    .subtitle {{ color: #666; margin-bottom: 25px; font-size: 14px; }}
    .card {{ background: #fff; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); padding: 24px; margin-bottom: 24px; }}

    /* Comparison Table */
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th {{ background: #4472C4; color: #fff; padding: 10px 14px; text-align: center; font-weight: 600; }}
    th:first-child {{ text-align: left; }}
    td {{ padding: 8px 14px; border-bottom: 1px solid #e0e0e0; }}
    td:first-child {{ font-weight: 600; }}
    tr.wait-row {{ background: #FFF8E1; }}
    tr.stage-row {{ background: #fff; }}
    tr.total-row {{ background: #C6EFCE; font-weight: 700; font-size: 15px; }}
    tr.tpc-row {{ background: #C6EFCE; font-weight: 700; }}
    tr:hover {{ background: #E8F0FE; }}
    tr.total-row:hover, tr.tpc-row:hover {{ background: #B0D9B0; }}
    .time-val {{ font-family: 'Consolas', 'Courier New', monospace; color: #2F5496; font-weight: 600; text-align: center; }}

    /* Bar Charts */
    .chart-container {{ display: flex; flex-wrap: wrap; gap: 24px; }}
    .chart-card {{ flex: 1; min-width: 400px; background: #fff; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); padding: 20px; }}
    .bar-row {{ display: flex; align-items: center; margin-bottom: 6px; }}
    .bar-label {{ width: 280px; font-size: 12px; text-align: right; padding-right: 10px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .bar-wrapper {{ flex: 1; height: 24px; background: #f0f0f0; border-radius: 4px; position: relative; }}
    .bar {{ height: 100%; border-radius: 4px; transition: width 0.5s ease; min-width: 2px; }}
    .bar-value {{ margin-left: 8px; font-family: 'Consolas', monospace; font-size: 12px; color: #555; width: 120px; white-space: nowrap; }}
    .bar-stage {{ background: #4472C4; }}
    .bar-wait {{ background: #FFC000; }}

    /* Raw Data Tables */
    .raw-table {{ font-size: 13px; }}
    .raw-table th {{ background: #5B9BD5; font-size: 12px; padding: 6px 10px; }}
    .raw-table td {{ padding: 5px 10px; font-size: 12px; }}
    .guid-full {{ font-family: 'Consolas', monospace; font-size: 12px; color: #666; }}

    /* Legend */
    .legend {{ display: flex; gap: 20px; margin: 10px 0 20px 0; font-size: 13px; }}
    .legend-item {{ display: flex; align-items: center; gap: 6px; }}
    .legend-box {{ width: 16px; height: 16px; border-radius: 3px; }}
</style>
</head>
<body>
<div class="container">
    <h1>Performance Comparison Report</h1>
    <p class="subtitle">Comparing {len(all_runs)} job run(s) | Generated from Azure SQL data</p>
"""

    # === SECTION 1: Side-by-side Comparison Table ===
    html += """
    <div class="card">
        <h2>Side-by-Side Timing Comparison</h2>
        <div class="legend">
            <div class="legend-item"><div class="legend-box" style="background:#D6E4F0"></div> Stage Processing</div>
            <div class="legend-item"><div class="legend-box" style="background:#FFF8E1"></div> Wait Time</div>
            <div class="legend-item"><div class="legend-box" style="background:#C6EFCE"></div> Summary</div>
        </div>
        <table>
            <thead><tr>
                <th>Stage</th>
"""
    for gl in guid_labels:
        html += f'                <th>{gl}</th>\n'
    html += '            </tr></thead>\n            <tbody>\n'

    for crow in comparison_rows:
        label = crow["label"]
        if label == "Total Processing Time":
            row_class = "total-row"
        elif label == "Time per Case":
            row_class = "tpc-row"
        elif "Wait" in label:
            row_class = "wait-row"
        else:
            row_class = "stage-row"

        html += f'            <tr class="{row_class}"><td>{label}</td>'
        for v in crow["values"]:
            html += f'<td class="time-val">{v}</td>'
        html += '</tr>\n'

    html += '            </tbody>\n        </table>\n    </div>\n'

    # === SECTION 2: Per-Run Bar Charts ===
    html += '    <div class="card">\n        <h2>Stage Breakdown per Run</h2>\n        <div class="chart-container">\n'

    for r_idx, run in enumerate(all_runs):
        chart_data = _build_bar_chart_data(run)
        max_secs = max((d["seconds"] for d in chart_data), default=1)
        if max_secs == 0:
            max_secs = 1

        html += f"""        <div class="chart-card">
            <h3>{guid_labels[r_idx]}</h3>
            <p class="guid-full">GUID: {run["job_guid"]}</p>
            <br>
"""
        for d in chart_data:
            pct = (d["seconds"] / max_secs) * 100
            bar_class = "bar-wait" if d["type"] == "wait" else "bar-stage"
            html += f"""            <div class="bar-row">
                <div class="bar-label">{d["label"]}</div>
                <div class="bar-wrapper"><div class="bar {bar_class}" style="width:{pct:.1f}%"></div></div>
                <div class="bar-value">{d["formatted"]}</div>
            </div>
"""
        # Total bar
        total_fmt = _fmt_td(run["total_time"])
        html += f"""            <div class="bar-row" style="margin-top:8px; border-top: 2px solid #ccc; padding-top:8px;">
                <div class="bar-label" style="font-weight:700;">Total Processing Time</div>
                <div class="bar-wrapper"><div class="bar bar-stage" style="width:100%; background:#006100;"></div></div>
                <div class="bar-value" style="font-weight:700; color:#006100;">{total_fmt}</div>
            </div>
"""
        if run["case_count"] > 0:
            tpc = _fmt_td(run["total_time"] / run["case_count"])
            html += f"""            <div class="bar-row">
                <div class="bar-label" style="font-weight:700;">Time per Case ({run["case_count"]})</div>
                <div class="bar-wrapper"></div>
                <div class="bar-value" style="font-weight:700; color:#006100;">{tpc}</div>
            </div>
"""
        html += '        </div>\n'

    html += '        </div>\n    </div>\n'

    # === SECTION 3: Raw Data per Run ===
    html += '    <div class="card">\n        <h2>Raw Job Steps Data</h2>\n'

    display_cols = ["JobStepId", "JobStepName", "Description",
                    "JobDefinitionStepStatusId", "UpdatedTimestamp"]

    for r_idx, run in enumerate(all_runs):
        html += f"""        <h3>{guid_labels[r_idx]}</h3>
        <p class="guid-full">GUID: {run["job_guid"]}</p>
        <table class="raw-table">
            <thead><tr>
"""
        for col in display_cols:
            html += f'                <th>{col}</th>\n'
        html += '            </tr></thead>\n            <tbody>\n'

        for _, data_row in run["df"].iterrows():
            html += '            <tr>'
            for col in display_cols:
                val = data_row.get(col)
                if hasattr(val, "strftime"):
                    val = val.strftime("%Y-%m-%d %H:%M:%S")
                html += f'<td>{val}</td>'
            html += '</tr>\n'

        html += '            </tbody>\n        </table>\n        <br>\n'

    html += '    </div>\n</div>\n</body>\n</html>'

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"HTML report generated: {output_path}")
    return output_path
