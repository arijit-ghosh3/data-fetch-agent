from datetime import timedelta
from collections import defaultdict
from processing.transformer import (
    _assign_rows_to_stages, _compute_timings, _fmt_td,
)

STAGE_LABELS = [
    "CREATE JOB >>",
    "WAIT TIME 0 (START - WJ1)",
    "WEB JOB 1 Processing Time",
    "WAIT TIME 1 (WJ2 - WJ1)",
    "WEB JOB 2 Processing Time",
    "WAIT TIME 2 (WJ3 - WJ2)",
    "WEB JOB 3 Processing Time",
    "TOTAL PROCESSING TIME",
    "AVG. Time per Input Case",
]

BAR_CHART_STAGES = [
    "WEB JOB 1 Processing Time",
    "WEB JOB 2 Processing Time",
    "WEB JOB 3 Processing Time",
    "TOTAL PROCESSING TIME",
    "AVG. Time per Input Case",
]

CHART_COLORS = [
    "#4472C4", "#ED7D31", "#A5A5A5", "#FFC000", "#5B9BD5",
    "#70AD47", "#264478", "#9B59B6", "#E74C3C", "#1ABC9C",
    "#F39C12", "#8E44AD", "#2ECC71", "#E67E22",
]


def _td_to_seconds(td):
    if td is None:
        return 0
    return td.total_seconds()


def _extract_stage_timings(run):
    """Extract normalized stage timings matching the template row labels."""
    timings = run["timings"]
    result = {}
    result["CREATE JOB >>"] = "TRIGGER"

    for t in timings:
        label = t["label"]
        if t["type"] == "wait":
            if "Web Job 1" in label:
                result["WAIT TIME 0 (START - WJ1)"] = t["duration"]
            elif "Web Job 2" in label:
                result["WAIT TIME 1 (WJ2 - WJ1)"] = t["duration"]
            elif "Web Job 3" in label:
                result["WAIT TIME 2 (WJ3 - WJ2)"] = t["duration"]
        elif t["type"] == "stage":
            if "Load Data" in label or "Web Job 1" in label:
                result["WEB JOB 1 Processing Time"] = t["duration"]
            elif "Validate" in label or "Web Job 2" in label:
                result["WEB JOB 2 Processing Time"] = t["duration"]
            elif "Create Case" in label or "Web Job 3" in label:
                result["WEB JOB 3 Processing Time"] = t["duration"]

    result["TOTAL PROCESSING TIME"] = run["total_time"]
    cc = run["case_count"]
    if cc and cc > 0:
        result["AVG. Time per Input Case"] = run["total_time"] / cc
    else:
        result["AVG. Time per Input Case"] = timedelta(0)

    return result


def _group_runs_by_case_count(all_runs):
    groups = defaultdict(list)
    for run in all_runs:
        groups[run["case_count"]].append(run)
    return dict(sorted(groups.items()))


def _compute_averages(runs):
    """Compute average timedelta for each stage across runs."""
    stage_totals = defaultdict(list)
    for run in runs:
        st = _extract_stage_timings(run)
        for label in STAGE_LABELS:
            val = st.get(label)
            if isinstance(val, timedelta):
                stage_totals[label].append(val)
    averages = {}
    for label, vals in stage_totals.items():
        if vals:
            total_secs = sum(v.total_seconds() for v in vals) / len(vals)
            averages[label] = timedelta(seconds=total_secs)
    return averages


def _generate_insights(grouped_runs, all_runs):
    """Generate business insights from the data."""
    points = []

    total_runs = len(all_runs)
    points.append(f"Total {total_runs} job run(s) analyzed across {len(grouped_runs)} different case count configuration(s).")

    for cc, runs in grouped_runs.items():
        avgs = _compute_averages(runs)
        total_avg = avgs.get("TOTAL PROCESSING TIME")
        tpc_avg = avgs.get("AVG. Time per Input Case")
        if total_avg:
            points.append(
                f"For {cc}-case runs ({len(runs)} runs): Average total processing time is {_fmt_td(total_avg)}."
            )
        if tpc_avg:
            points.append(
                f"For {cc}-case runs: Average time per case is {_fmt_td(tpc_avg)}."
            )

        # Identify bottleneck
        wj_stages = ["WEB JOB 1 Processing Time", "WEB JOB 2 Processing Time", "WEB JOB 3 Processing Time"]
        max_stage = None
        max_secs = 0
        for s in wj_stages:
            if s in avgs:
                secs = avgs[s].total_seconds()
                if secs > max_secs:
                    max_secs = secs
                    max_stage = s
        if max_stage:
            points.append(
                f"For {cc}-case runs: '{max_stage}' is the most time-consuming stage "
                f"(avg {_fmt_td(avgs[max_stage])}), indicating a potential optimization target."
            )

        # Wait time analysis
        wait_stages = ["WAIT TIME 0 (START - WJ1)", "WAIT TIME 1 (WJ2 - WJ1)", "WAIT TIME 2 (WJ3 - WJ2)"]
        total_wait = timedelta(0)
        for w in wait_stages:
            if w in avgs:
                total_wait += avgs[w]
        if total_avg and total_avg.total_seconds() > 0:
            wait_pct = (total_wait.total_seconds() / total_avg.total_seconds()) * 100
            points.append(
                f"For {cc}-case runs: Total wait time accounts for {wait_pct:.1f}% of the overall processing time "
                f"({_fmt_td(total_wait)} out of {_fmt_td(total_avg)})."
            )

    return points


def _render_comparison_table(runs, case_count):
    """Render a comparison table matching the FinalReportTemplate format."""
    html = '<table class="comp-table">\n'

    # Header row 1: FILES label + run columns
    html += '<thead>\n'
    html += f'<tr><th rowspan="3" class="stage-header">FILES ({case_count} REC)</th>\n'
    for i, run in enumerate(runs):
        html += f'<th class="run-header">{i+1}. JOB GUID</th>\n'
    html += '</tr>\n'

    # Header row 2: JOB GUIDs
    html += '<tr>\n'
    for run in runs:
        html += f'<td class="guid-cell">{run["job_guid"]}</td>\n'
    html += '</tr>\n'

    # Header row 3: case counts
    html += '<tr>\n'
    for run in runs:
        html += f'<td class="case-cell">({run["case_count"]} REC)</td>\n'
    html += '</tr>\n'
    html += '</thead>\n<tbody>\n'

    # Data rows
    all_stage_timings = [_extract_stage_timings(r) for r in runs]

    for label in STAGE_LABELS:
        if label == "TOTAL PROCESSING TIME":
            row_cls = 'class="total-row"'
        elif label == "AVG. Time per Input Case":
            row_cls = 'class="avg-row"'
        elif "WAIT" in label:
            row_cls = 'class="wait-row"'
        elif "CREATE JOB" in label:
            row_cls = 'class="trigger-row"'
        else:
            row_cls = 'class="stage-row"'

        html += f'<tr {row_cls}><td class="label-cell">{label}</td>\n'
        for st in all_stage_timings:
            val = st.get(label, "")
            if isinstance(val, timedelta):
                val = _fmt_td(val)
            html += f'<td class="time-val">{val}</td>\n'
        html += '</tr>\n'

    # Format row
    html += '<tr class="format-row"><td class="label-cell">(hh:mm:ss.000)</td>\n'
    for _ in runs:
        html += '<td></td>\n'
    html += '</tr>\n'

    html += '</tbody>\n</table>\n'
    return html


def _render_avg_table(runs, case_count):
    """Render average timing table for a group."""
    avgs = _compute_averages(runs)
    html = '<table class="avg-table">\n'
    html += '<thead><tr><th>Stage</th><th>Average Duration (hh:mm:ss.000)</th></tr></thead>\n'
    html += '<tbody>\n'
    for label in STAGE_LABELS:
        if label == "CREATE JOB >>":
            continue
        val = avgs.get(label)
        if val is None:
            continue
        if label == "TOTAL PROCESSING TIME":
            cls = 'class="total-row"'
        elif label == "AVG. Time per Input Case":
            cls = 'class="avg-row"'
        elif "WAIT" in label:
            cls = 'class="wait-row"'
        else:
            cls = 'class="stage-row"'
        html += f'<tr {cls}><td class="label-cell">{label}</td>'
        html += f'<td class="time-val">{_fmt_td(val)}</td></tr>\n'
    html += '</tbody>\n</table>\n'
    return html


def _render_bar_chart_svg(grouped_runs):
    """Render grouped bar chart comparing stages across all Job GUIDs using inline SVG."""
    all_runs_flat = []
    for cc, runs in grouped_runs.items():
        all_runs_flat.extend(runs)

    num_runs = len(all_runs_flat)
    stages = BAR_CHART_STAGES
    num_stages = len(stages)

    # Collect data
    chart_data = []
    for run in all_runs_flat:
        st = _extract_stage_timings(run)
        run_data = []
        for s in stages:
            val = st.get(s)
            secs = _td_to_seconds(val) if isinstance(val, timedelta) else 0
            run_data.append(secs)
        chart_data.append({
            "guid_short": run["job_guid"][:8],
            "case_count": run["case_count"],
            "values": run_data,
        })

    max_val = max(
        (v for cd in chart_data for v in cd["values"]),
        default=1,
    )
    if max_val == 0:
        max_val = 1

    # Chart dimensions
    left_margin = 250
    right_margin = 40
    top_margin = 40
    bottom_margin = 120
    group_width = max(num_runs * 28 + 20, 80)
    chart_width = left_margin + num_stages * group_width + right_margin
    chart_height = 400
    plot_height = chart_height - top_margin - bottom_margin

    svg = f'<svg width="{chart_width}" height="{chart_height}" xmlns="http://www.w3.org/2000/svg">\n'
    svg += f'<rect width="{chart_width}" height="{chart_height}" fill="#fafafa" rx="8"/>\n'

    # Y-axis gridlines and labels
    num_grid = 5
    for i in range(num_grid + 1):
        y = top_margin + plot_height - (i / num_grid) * plot_height
        val_secs = (i / num_grid) * max_val
        h = int(val_secs // 3600)
        m = int((val_secs % 3600) // 60)
        s = int(val_secs % 60)
        label = f"{h:02d}:{m:02d}:{s:02d}"
        svg += f'<line x1="{left_margin}" y1="{y}" x2="{chart_width - right_margin}" y2="{y}" stroke="#e0e0e0" stroke-width="1"/>\n'
        svg += f'<text x="{left_margin - 10}" y="{y + 4}" text-anchor="end" font-size="11" fill="#666">{label}</text>\n'

    # Bars
    for s_idx, stage in enumerate(stages):
        group_x = left_margin + s_idx * group_width + 10
        bar_width = max(20, (group_width - 20) / num_runs - 4)

        for r_idx, cd in enumerate(chart_data):
            val = cd["values"][s_idx]
            bar_h = (val / max_val) * plot_height if max_val > 0 else 0
            x = group_x + r_idx * (bar_width + 4)
            y = top_margin + plot_height - bar_h
            color = CHART_COLORS[r_idx % len(CHART_COLORS)]
            svg += f'<rect x="{x}" y="{y}" width="{bar_width}" height="{bar_h}" fill="{color}" rx="2">'
            svg += f'<title>{cd["guid_short"]}... ({cd["case_count"]} cases): {_fmt_td(timedelta(seconds=val))}</title></rect>\n'

        # Stage label (rotated)
        label_x = group_x + (group_width - 20) / 2
        label_y = top_margin + plot_height + 12
        svg += f'<text x="{label_x}" y="{label_y}" text-anchor="end" font-size="11" fill="#333" '
        svg += f'transform="rotate(-35, {label_x}, {label_y})">{stage}</text>\n'

    svg += '</svg>\n'

    # Legend
    legend = '<div class="chart-legend">\n'
    for r_idx, cd in enumerate(chart_data):
        color = CHART_COLORS[r_idx % len(CHART_COLORS)]
        legend += f'<span class="legend-item"><span class="legend-box" style="background:{color}"></span>'
        legend += f'{cd["guid_short"]}... ({cd["case_count"]} cases)</span>\n'
    legend += '</div>\n'

    return svg + legend


def generate_html_report(all_runs, output_path):
    grouped = _group_runs_by_case_count(all_runs)
    insights = _generate_insights(grouped, all_runs)

    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>GDC Bulk Case Creation Result</title>
<style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #f0f2f5; color: #333; padding: 20px; }
    .container { max-width: 1600px; margin: 0 auto; }
    h1 { color: #1a3a6b; margin-bottom: 6px; font-size: 30px; text-align: center; }
    h2 { color: #2F5496; margin: 30px 0 15px 0; font-size: 20px; border-bottom: 2px solid #4472C4; padding-bottom: 6px; }
    h3 { color: #2F5496; margin: 20px 0 10px 0; font-size: 17px; }
    .subtitle { color: #666; margin-bottom: 25px; font-size: 14px; text-align: center; }
    .card { background: #fff; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.08); padding: 24px; margin-bottom: 24px; }

    /* Comparison Table - matching FinalReportTemplate */
    .comp-table { width: 100%; border-collapse: collapse; font-size: 13px; margin-bottom: 16px; }
    .comp-table th, .comp-table td { border: 1px solid #b0b0b0; padding: 7px 12px; }
    .stage-header { background: #2F5496; color: #fff; font-size: 14px; text-align: left; min-width: 280px; }
    .run-header { background: #4472C4; color: #fff; font-size: 12px; text-align: center; min-width: 180px; }
    .guid-cell { background: #D6E4F0; text-align: center; font-family: 'Consolas', monospace; font-size: 11px; word-break: break-all; }
    .case-cell { background: #E8F0FE; text-align: center; font-weight: 600; font-size: 12px; }
    .label-cell { font-weight: 600; background: #f8f9fa; }
    .time-val { font-family: 'Consolas', 'Courier New', monospace; color: #2F5496; font-weight: 600; text-align: center; }
    .wait-row { background: #FFF8E1; }
    .stage-row { background: #ffffff; }
    .trigger-row { background: #E8F0FE; }
    .total-row { background: #C6EFCE; font-weight: 700; }
    .total-row .time-val { color: #006100; font-size: 14px; }
    .avg-row { background: #C6EFCE; font-weight: 700; }
    .avg-row .time-val { color: #006100; }
    .format-row { background: #f0f0f0; font-style: italic; color: #888; font-size: 11px; }

    /* Average Table */
    .avg-table { width: 500px; border-collapse: collapse; font-size: 13px; margin: 10px 0 20px 0; }
    .avg-table th { background: #2F5496; color: #fff; padding: 8px 14px; text-align: center; }
    .avg-table th:first-child { text-align: left; }
    .avg-table td { border: 1px solid #b0b0b0; padding: 6px 12px; }

    /* Chart */
    .chart-wrapper { overflow-x: auto; margin: 20px 0; }
    .chart-legend { display: flex; flex-wrap: wrap; gap: 14px; margin: 14px 0; font-size: 12px; }
    .legend-item { display: flex; align-items: center; gap: 5px; }
    .legend-box { width: 14px; height: 14px; border-radius: 3px; display: inline-block; }

    /* Raw Data */
    .raw-table { width: 100%; border-collapse: collapse; font-size: 12px; margin: 10px 0 20px 0; }
    .raw-table th { background: #5B9BD5; color: #fff; padding: 6px 10px; font-size: 12px; }
    .raw-table td { padding: 5px 10px; border: 1px solid #ddd; }
    .guid-full { font-family: 'Consolas', monospace; font-size: 11px; color: #666; }

    /* Insights */
    .insight-list { margin: 10px 0 0 20px; }
    .insight-list li { margin-bottom: 8px; line-height: 1.6; font-size: 14px; }
    .insight-list li strong { color: #2F5496; }

    .tab-nav { display: flex; gap: 0; margin-bottom: 0; }
    .tab-btn { padding: 10px 24px; background: #e0e0e0; border: 1px solid #ccc; border-bottom: none;
               cursor: pointer; font-size: 14px; font-weight: 600; color: #555; border-radius: 8px 8px 0 0; }
    .tab-btn.active { background: #fff; color: #2F5496; border-bottom: 2px solid #fff; }
    .tab-content { display: none; padding: 20px 0; }
    .tab-content.active { display: block; }
</style>
</head>
<body>
<div class="container">
    <h1>GDC Bulk Case Creation Result</h1>
"""

    html += f'    <p class="subtitle">Analyzing {len(all_runs)} job run(s) across {len(grouped)} case count group(s) | All timings in hh:mm:ss.000</p>\n'

    # === SECTION 1: Business Insights ===
    html += '    <div class="card">\n'
    html += '        <h2>Key Insights</h2>\n'
    html += '        <ul class="insight-list">\n'
    for pt in insights:
        html += f'            <li>{pt}</li>\n'
    html += '        </ul>\n'
    html += '    </div>\n'

    # === SECTION 2: Grouped by Case Count - Tabs ===
    html += '    <div class="card">\n'
    html += '        <h2>Performance Results by Case Count</h2>\n'

    # Tab navigation
    html += '        <div class="tab-nav">\n'
    case_counts = list(grouped.keys())
    for i, cc in enumerate(case_counts):
        active = ' active' if i == 0 else ''
        html += f'            <div class="tab-btn{active}" onclick="switchTab({i})" id="tab-btn-{i}">{cc} Cases ({len(grouped[cc])} runs)</div>\n'
    html += '        </div>\n'

    # Tab content
    for i, (cc, runs) in enumerate(grouped.items()):
        active = ' active' if i == 0 else ''
        html += f'        <div class="tab-content{active}" id="tab-{i}">\n'

        # Comparison table
        html += f'            <h3>Side-by-Side Comparison ({cc} Cases)</h3>\n'
        html += '            <div style="overflow-x:auto;">\n'
        html += _render_comparison_table(runs, cc)
        html += '            </div>\n'

        # Average table
        html += f'            <h3>Average Processing Time ({cc} Cases, {len(runs)} runs)</h3>\n'
        html += _render_avg_table(runs, cc)

        # Raw data
        display_cols = ["JobStepId", "JobStepName", "Description",
                        "JobDefinitionStepStatusId", "UpdatedTimestamp"]
        html += f'            <h3>Raw Job Steps Data ({cc} Cases)</h3>\n'
        for r_idx, run in enumerate(runs):
            html += f'            <p><strong>Run {r_idx+1}</strong> <span class="guid-full">GUID: {run["job_guid"]}</span></p>\n'
            html += '            <table class="raw-table"><thead><tr>\n'
            for col in display_cols:
                html += f'                <th>{col}</th>\n'
            html += '            </tr></thead><tbody>\n'
            for _, data_row in run["df"].iterrows():
                html += '            <tr>'
                for col in display_cols:
                    val = data_row.get(col)
                    if hasattr(val, "strftime"):
                        val = val.strftime("%Y-%m-%d %H:%M:%S")
                    html += f'<td>{val}</td>'
                html += '</tr>\n'
            html += '            </tbody></table><br>\n'

        html += '        </div>\n'

    html += '    </div>\n'

    # === SECTION 3: Bar Chart Comparison ===
    html += '    <div class="card">\n'
    html += '        <h2>Stage-wise Comparison Across All Runs</h2>\n'
    html += '        <div class="chart-wrapper">\n'
    html += _render_bar_chart_svg(grouped)
    html += '        </div>\n'
    html += '    </div>\n'

    # JavaScript for tabs
    html += """
<script>
function switchTab(idx) {
    document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
    document.getElementById('tab-' + idx).classList.add('active');
    document.getElementById('tab-btn-' + idx).classList.add('active');
}
</script>
"""

    html += '</div>\n</body>\n</html>'

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"HTML report generated: {output_path}")
    return output_path
