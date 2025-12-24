import requests
import pandas as pd


def build_slack_summary(df: pd.DataFrame, categories_en) -> str:
    d = df.copy()
    d["BANK"] = d["BANK"].astype(str).str.strip()
    d["TYPE"] = d["TYPE"].astype(str).str.strip()
    d = d[d["TYPE"].isin(categories_en)]

    pivot = (
        d.pivot_table(index="BANK", columns="TYPE", values="TEXT", aggfunc="count", fill_value=0)
        .reindex(columns=categories_en, fill_value=0)
    )
    col_totals = pivot.sum(axis=0)

    header = [
        ":bar_chart: *Tableau Bank Ads – Data Refresh* :white_check_mark:",
        f"Period: {d['DATE'].iloc[0] if len(d) else ''} | Rows: {len(d)} | Banks: {d['BANK'].nunique() if len(d) else 0}",
        "",
        "Category leaders (share of category)",
        "",
    ]

    rows = []
    for typ in pivot.columns:
        total = int(col_totals[typ])
        if total == 0:
            continue

        leader_bank = pivot[typ].idxmax()
        leader_count = int(pivot.loc[leader_bank, typ])
        share = round((leader_count / total) * 100)

        rows.append((typ, leader_bank, f"{share}%"))

    rows = [r for r in rows if r[0] != "Other"]

    w1 = max(len("Product"), *(len(r[0]) for r in rows)) if rows else len("Product")
    w2 = max(len("Leader Bank"), *(len(r[1]) for r in rows)) if rows else len("Leader Bank")

    table_lines = []
    table_lines.append(f"{'Product':<{w1}}  {'Leader Bank':<{w2}}  Share")
    table_lines.append("-" * (w1 + 2 + w2 + 2 + len("Share")))
    for product, leader, share in rows:
        table_lines.append(f"{product:<{w1}}  {leader:<{w2}}  {share:>5}")

    table_block = "```" + "\n" + "\n".join(table_lines) + "\n```"

    return "\n".join(header) + table_block


def notify_slack(webhook: str, message: str):
    if not webhook:
        print("[SLACK] SLACK_WEBHOOK_URL not set - skipping")
        return
    requests.post(webhook, json={"text": message}, timeout=10)
