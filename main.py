import os
import re
import time
from pathlib import Path
from dataclasses import dataclass
from typing import List

import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI

from pipeline.serpapi import fetch_preview_urls, download_image
from pipeline.ocr import ocr_and_classify
from pipeline.hyper import export_hyper
from pipeline.slack import build_slack_summary, notify_slack
from pipeline.tableau import publish_to_tableau


# Period (set manually)
START_DATE = "20251101"
END_DATE = "20251130"
PERIOD_LABEL = f"{START_DATE}_to_{END_DATE}"


def end_date_to_iso(value: str) -> str:
    if not re.fullmatch(r"\d{8}", value):
        raise ValueError("END_DATE must be YYYYMMDD")
    return f"{value[:4]}-{value[4:6]}-{value[6:]}"


DATE_VALUE = end_date_to_iso(END_DATE)

# Paths
OUT_DIR = Path("out")
DATA_DIR = OUT_DIR / "data"
IMAGES_DIR = OUT_DIR / "images"
DATA_DIR.mkdir(parents=True, exist_ok=True)
IMAGES_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_CSV = DATA_DIR / f"bank_ads_{DATE_VALUE}.csv"
OUTPUT_HYPER = DATA_DIR / f"bank_ads_{DATE_VALUE}.hyper"
HYPER_TABLE = "ads"

# Env
load_dotenv()

SERPAPI_KEY = os.getenv("SERPAPI_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not SERPAPI_KEY:
    raise SystemExit("Missing SERPAPI_KEY")
if not OPENAI_API_KEY:
    raise SystemExit("Missing OPENAI_API_KEY")

# OpenAI config
client = OpenAI(api_key=OPENAI_API_KEY)
MODEL = "gpt-5-chat-latest"

CATEGORIES_EN = [
    "Deposits",
    "Mortgage Loan",
    "Consumer Loan",
    "Overdraft",
    "Credit Card",
    "Banking Package",
    "Business Loan",
    "Promotional Offer",
    "Mobile Banking",
    "Investing",
    "Other",
]

BANK_LABELS = {
    "DSK": "DSK",
    "UBB": "UBB",
    "POSTBANK": "Postbank",
}

MAX_RETRIES = 3
BASE_SLEEP = 1.0
PAUSE_BETWEEN_CALLS = 0.25

ADVERTISERS = {
    "AR02588471511060840449": "DSK",
    "AR14517726923544592385": "UBB",
    "AR08110226567973568513": "POSTBANK",
    "AR00474820660481228801": "POSTBANK",
    "AR09013917442585395201": "POSTBANK",
}


# Main
@dataclass
class Row:
    BANK: str
    TEXT: str
    TYPE: str
    DATE: str


def main():
    # Publish-only mode: use CSV for the summary (pandas cannot read .hyper directly)
    if OUTPUT_HYPER.exists():
        print(f"[MODE] publish-only (existing hyper): {OUTPUT_HYPER}")
        if OUTPUT_CSV.exists():
            df_csv = pd.read_csv(OUTPUT_CSV, dtype=str, keep_default_na=False)
            summary = build_slack_summary(df_csv, CATEGORIES_EN)
        else:
            summary = (
                "📊 *Tableau Bank Ads – Data Refresh* ✅\n"
                f"Period: {START_DATE} → {END_DATE}\n"
                "Status: ✅ Published successfully"
            )

        publish_to_tableau(
            OUTPUT_HYPER,
            summary,
            server_url=os.getenv("TABLEAU_SERVER_URL"),
            site_id=os.getenv("TABLEAU_SITE_ID"),
            pat_name=os.getenv("TABLEAU_PAT_NAME"),
            pat_secret=os.getenv("TABLEAU_PAT_SECRET"),
            project_name=os.getenv("TABLEAU_PROJECT_NAME", "Default"),
            datasource_name=os.getenv("TABLEAU_DATASOURCE_NAME", "bank_ads_latest"),
            slack_webhook=os.getenv("SLACK_WEBHOOK_URL", ""),
            notify_slack_fn=notify_slack,
        )
        return

    all_rows: List[Row] = []

    for adv_id, bank in ADVERTISERS.items():
        urls = fetch_preview_urls(SERPAPI_KEY, adv_id, START_DATE, END_DATE)
        bank_folder = IMAGES_DIR / bank
        bank_folder.mkdir(parents=True, exist_ok=True)

        for url in urls:
            try:
                img_path = download_image(url, bank_folder)
                res = ocr_and_classify(client, MODEL, CATEGORIES_EN, img_path)
                if not res["text"]:
                    continue

                all_rows.append(
                    Row(
                        BANK=BANK_LABELS[bank],
                        TEXT=res["text"],
                        TYPE=res["type"],
                        DATE=DATE_VALUE,
                    )
                )
                time.sleep(PAUSE_BETWEEN_CALLS)
            except Exception:
                continue

    if not all_rows:
        print("No rows produced.")
        return

    df = pd.DataFrame([r.__dict__ for r in all_rows], columns=["BANK", "TEXT", "TYPE", "DATE"])

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"Saved CSV: {OUTPUT_CSV} (rows={len(df)})")

    export_hyper(df, OUTPUT_HYPER, HYPER_TABLE)
    print(f"Saved HYPER: {OUTPUT_HYPER} (table={HYPER_TABLE})")

    summary = build_slack_summary(df, CATEGORIES_EN)

    publish_to_tableau(
        OUTPUT_HYPER,
        summary,
        server_url=os.getenv("TABLEAU_SERVER_URL"),
        site_id=os.getenv("TABLEAU_SITE_ID"),
        pat_name=os.getenv("TABLEAU_PAT_NAME"),
        pat_secret=os.getenv("TABLEAU_PAT_SECRET"),
        project_name=os.getenv("TABLEAU_PROJECT_NAME", "Default"),
        datasource_name=os.getenv("TABLEAU_DATASOURCE_NAME", "bank_ads_latest"),
        slack_webhook=os.getenv("SLACK_WEBHOOK_URL", ""),
        notify_slack_fn=notify_slack,
    )


if __name__ == "__main__":
    main()
