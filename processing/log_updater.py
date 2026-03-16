from datetime import datetime, timedelta
from collections import defaultdict
import openpyxl
from processing.transformer import _fmt_td
from processing.html_report import _extract_stage_timings


def _next_sl_no(ws):
    """Find the next SL.NO from the RUN LOG sheet."""
    last = 0
    for row in range(4, ws.max_row + 1):
        v = ws.cell(row=row, column=2).value
        if v is not None and str(v).strip().isdigit():
            last = int(v)
    return last + 1


def _next_empty_row(ws):
    """Find the next empty row in the RUN LOG sheet."""
    for row in range(4, ws.max_row + 2):
        if ws.cell(row=row, column=6).value is None:
            return row
    return ws.max_row + 1


def _fmt_duration(td):
    """Format timedelta as hh:mm:ss.000000 matching RUN LOG format."""
    if td is None:
        return ""
    total = td.total_seconds()
    sign = "-" if total < 0 else ""
    total = abs(total)
    h, rem = divmod(int(total), 3600)
    m, s = divmod(rem, 60)
    micro = int((total - int(total)) * 1000000)
    return f"{sign}{h:02d}:{m:02d}:{s:02d}.{micro:06d}"


def _get_stage_td(run, stage_label):
    """Get a stage's timedelta from run's extracted timings."""
    st = _extract_stage_timings(run)
    val = st.get(stage_label)
    if isinstance(val, timedelta):
        return val
    return None


def update_run_log(historical_file, all_runs, test_name=None):
    """Append current run data to GDC_RUN_LOG.xlsx.

    1. Add rows to the 'RUN LOG' sheet (one per GUID).
    2. Add a new detail sheet with the comparison table format.
    """
    wb = openpyxl.load_workbook(historical_file)

    # Group runs by case count for the detail sheet
    grouped = defaultdict(list)
    for run in all_runs:
        grouped[run["case_count"]].append(run)

    today = datetime.now().strftime("%Y-%m-%d")
    if test_name is None:
        test_name = f"Run_{datetime.now().strftime('%d%b%Y')}"

    # === 1. Update RUN LOG sheet ===
    ws_log = wb["RUN LOG"]
    sl_no = _next_sl_no(ws_log)
    insert_row = _next_empty_row(ws_log)

    first_guid = True
    for run in all_runs:
        st = _extract_stage_timings(run)
        total_td = run["total_time"]
        wj1 = _get_stage_td(run, "WEB JOB 1 Processing Time")
        wj2 = _get_stage_td(run, "WEB JOB 2 Processing Time")
        wj3 = _get_stage_td(run, "WEB JOB 3 Processing Time")
        cc = run["case_count"]
        tpc = total_td.total_seconds() / cc if cc > 0 else 0

        if first_guid:
            ws_log.cell(row=insert_row, column=2, value=sl_no)
            ws_log.cell(row=insert_row, column=3, value=today)
            ws_log.cell(row=insert_row, column=4, value="AUTOMATED")
            ws_log.cell(row=insert_row, column=5, value=test_name)
            first_guid = False

        ws_log.cell(row=insert_row, column=6, value=_fmt_duration(total_td))
        ws_log.cell(row=insert_row, column=7, value=_fmt_duration(wj1) if wj1 else "NA")
        ws_log.cell(row=insert_row, column=8, value=_fmt_duration(wj2) if wj2 else "NA")
        ws_log.cell(row=insert_row, column=9, value=_fmt_duration(wj3) if wj3 else "NA")
        ws_log.cell(row=insert_row, column=10, value="NA")
        ws_log.cell(row=insert_row, column=11, value=round(tpc, 3))
        insert_row += 1

    # === 2. Add a new detail sheet ===
    sheet_name = f"Auto_{datetime.now().strftime('%d%b%Y_%H%M')}"
    # Truncate to 31 chars (Excel limit)
    sheet_name = sheet_name[:31]
    ws_detail = wb.create_sheet(title=sheet_name)

    # Build the detail sheet matching existing format
    for cc, runs in sorted(grouped.items()):
        total_rec = cc * len(runs)

        # Row 1: empty
        ws_detail.cell(row=1, column=1, value="")

        # Row 2: FILES header + per-GUID columns
        ws_detail.cell(row=2, column=1, value=f"FILES ({total_rec} REC)")
        for i, run in enumerate(runs):
            header = f"JOB GUID - {run['job_guid']}\n({cc} REC)"
            ws_detail.cell(row=2, column=2 + i, value=header)

        # Row 3: STATUS
        ws_detail.cell(row=3, column=1, value="STATUS")
        for i, run in enumerate(runs):
            ws_detail.cell(row=3, column=2 + i, value="Job successfully completed")

        # Row 4: CREATE JOB
        ws_detail.cell(row=4, column=1, value="CREATE JOB  >>")
        ws_detail.cell(row=4, column=2, value="TRIGGER")

        # Stage rows
        stage_rows = [
            ("WAIT TIME 0 (START - WJ1) -- >", "WAIT TIME 0 (START - WJ1)"),
            ("WEB JOB 1 Processing time", "WEB JOB 1 Processing Time"),
            ("WAIT TIME 1 (WJ2 - WJ1) -- >", "WAIT TIME 1 (WJ2 - WJ1)"),
            ("WEB JOB 2 Processing time", "WEB JOB 2 Processing Time"),
            ("WAIT TIME 2 (WJ3 - WJ2) -- >", "WAIT TIME 2 (WJ3 - WJ2)"),
            ("WEB JOB 3 Processing time", "WEB JOB 3 Processing Time"),
        ]

        row_num = 5
        for sheet_label, key in stage_rows:
            ws_detail.cell(row=row_num, column=1, value=sheet_label)
            for i, run in enumerate(runs):
                st = _extract_stage_timings(run)
                val = st.get(key)
                if isinstance(val, timedelta):
                    ws_detail.cell(row=row_num, column=2 + i, value=_fmt_duration(val))
                else:
                    ws_detail.cell(row=row_num, column=2 + i, value="")
            row_num += 1

        # WAIT TIME 3 (placeholder)
        ws_detail.cell(row=row_num, column=1, value="WAIT TIME 3 (WJ4 - WJ3) -- >")
        row_num += 1

        # RETRY JOB (placeholder)
        ws_detail.cell(row=row_num, column=1, value="RETRY JOB Processing time")
        row_num += 1

        # TOTAL PROCESSING TIME
        ws_detail.cell(row=row_num, column=1, value="TOTAL PROCESSING TIME \n(hh:mm:ss.000)")
        for i, run in enumerate(runs):
            ws_detail.cell(row=row_num, column=2 + i, value=_fmt_duration(run["total_time"]))
        row_num += 1

        # AVG Time per input case
        ws_detail.cell(row=row_num, column=1, value="AVG. Time per input case")
        for i, run in enumerate(runs):
            cc_val = run["case_count"]
            if cc_val > 0:
                tpc_td = run["total_time"] / cc_val
                ws_detail.cell(row=row_num, column=2 + i, value=_fmt_duration(tpc_td))

    wb.save(historical_file)
    print(f"GDC_RUN_LOG.xlsx updated: new rows in 'RUN LOG' + new sheet '{sheet_name}'")
    return sheet_name
