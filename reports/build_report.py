#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import duckdb
import matplotlib.pyplot as plt


def main() -> None:
    ap = argparse.ArgumentParser(description="Build a small markdown report from the dbt marts layer.")
    ap.add_argument("--db", required=True, help="Path to DuckDB database file.")
    ap.add_argument("--outdir", default="reports/outputs", help="Output directory for report artifacts.")
    args = ap.parse_args()

    db_path = Path(args.db)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path), read_only=True)

    kpis = con.execute(
        """
        select *
        from marts.mart_daily_kpis
        order by day desc
        limit 14
        """
    ).df()

    chart_df = con.execute(
        """
        select day, revenue, orders, pageviews
        from marts.mart_daily_kpis
        order by day
        """
    ).df()


    fig1 = plt.figure()
    plt.plot(chart_df["day"], chart_df["revenue"])
    plt.xticks(rotation=45, ha="right")
    plt.title("Revenue by day")
    plt.tight_layout()
    revenue_png = outdir / "revenue_by_day.png"
    plt.savefig(revenue_png, dpi=150)
    plt.close(fig1)

    fig2 = plt.figure()
    ax = plt.gca()

    ax.plot(chart_df["day"], chart_df["pageviews"])

    # <-- add these two lines
    ax.set_xlim(chart_df["day"].min(), chart_df["day"].max())
    ax.margins(x=0)

    plt.xticks(rotation=45, ha="right")
    plt.title("Pageviews by day")
    plt.tight_layout()
    pv_png = outdir / "pageviews_by_day.png"
    plt.savefig(pv_png, dpi=150)
    plt.close(fig2)


    md = []
    md.append("# Local Lakehouse Report")
    md.append("")
    md.append("This report is generated from `marts.mart_daily_kpis` after running `dbt build`.")
    md.append("")
    md.append("## Last 14 days (UTC)")
    md.append("")
    md.append(kpis.to_markdown(index=False))
    md.append("")
    md.append("## Charts")
    md.append("")
    md.append(f"![Revenue by day]({revenue_png.as_posix()})")
    md.append("")
    md.append(f"![Pageviews by day]({pv_png.as_posix()})")
    md.append("")

    report_path = outdir / "report.md"
    report_path.write_text("\n".join(md), encoding="utf-8")

    print(f"Wrote {report_path}")
    print(f"Wrote {revenue_png}")
    print(f"Wrote {pv_png}")


if __name__ == "__main__":
    main()

