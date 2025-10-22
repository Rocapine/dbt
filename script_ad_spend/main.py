import os
import sys
import csv
from typing import List, Optional
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from constant import (
    HISTORICAL_TIKTOK_IDS,
    HISTORICAL_META_IDS,
    NEW_AD_ACCOUNT_TIKTOK_IDS,
    NEW_AD_ACCOUNT_META_IDS,
    HISTORICAL_ASA_ORG_IDS,
    NEW_AD_ACCOUNT_ASA_ORG_IDS,
)
from tiktok_service import fetch_daily_spend_by_country_by_adgroup as tiktok_spend_by_country_by_adgroup
from meta_service import fetch_daily_spend_by_country as meta_spend_by_country
from asa_service import fetch_daily_spend_by_country_by_adgroup as asa_spend_by_country_by_adgroup
from bq_writer import write_rows_to_bigquery


def main(argv: List[str]) -> int:
    load_dotenv()

    args = argv[1:]
    TIKTOK_IDS = HISTORICAL_TIKTOK_IDS
    META_IDS = HISTORICAL_META_IDS
    ASA_IDS = HISTORICAL_ASA_ORG_IDS

    # parsed date args
    start_date_arg: Optional[str] = None
    end_date_arg: Optional[str] = None

    # flags
    to_bq = False
    bq_dataset = "AdSpend"
    bq_project = "rocadata"
    bq_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if "--to-bq" in args:
        to_bq = True
        args = [a for a in args if a != "--to-bq"]
    if "--new-ad-account" in args:
        args = [a for a in args if a != "--new-ad-account"]
        TIKTOK_IDS = NEW_AD_ACCOUNT_TIKTOK_IDS
        META_IDS = NEW_AD_ACCOUNT_META_IDS
        ASA_IDS = NEW_AD_ACCOUNT_ASA_ORG_IDS
    def is_yyyy_mm_dd(s: str) -> bool:
        try:
            datetime.strptime(s, "%Y-%m-%d")
            return True
        except Exception:
            return False

    if len(args) >= 2 and is_yyyy_mm_dd(args[0]) and is_yyyy_mm_dd(args[1]):
        start_date_arg, end_date_arg = args[0], args[1]
    else:
        # Default to yesterday (UTC) when dates are not supplied
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date_arg = yesterday
        end_date_arg = yesterday


    channels = {
        "tiktok": {
            "ids_map": TIKTOK_IDS,
            "table": "TestJobs",
            "fetch": tiktok_spend_by_country_by_adgroup,
        },
        "meta": {
            "ids_map": META_IDS,
            "table": "TestJobs",
            "fetch": meta_spend_by_country,
        },
        "asa": {
            "ids_map": ASA_IDS,
            "table": "TestJobs",
            "fetch": asa_spend_by_country_by_adgroup,
        },
    }

    writer = None if to_bq else csv.writer(sys.stdout)
    if writer:
        writer.writerow([
            "date",
            "app",
            "country",
            "spend",
            "currency",
            "campaign_id",
            "campaign_name",
            "adgroup_id",
            "adgroup_name",
        ])

    for chennel_name, cfg in channels.items():
        ids_map = cfg["ids_map"]
        table_for_channel = cfg["table"]
        fetch_fn = cfg["fetch"]

        apps_for_provider = sorted(ids_map.keys())

        for app in apps_for_provider:
            ids = [ids_map.get(app, "")]
            rows_country = fetch_fn(ids, start_date_str=start_date_arg, end_date_str=end_date_arg)

            if to_bq:
                # stream to BigQuery
                rows = (
                    {
                        "Date": r.date,
                        "Ad_Account": app,
                        "Country": r.country_code,
                        "Spend": float(f"{r.spend:.6f}"),
                        "Currency": getattr(r, "currency", "") or None,
                        "Campaign_id": getattr(r, "campaign_id", "") or None,
                        "Campaign_name": getattr(r, "campaign_name", "") or None,
                        "Adgroup_id": getattr(r, "adgroup_id", "") or None,
                        "Adgroup_name": getattr(r, "adgroup_name", "") or None,
                    }
                    for r in rows_country
                )
                if not bq_dataset:
                    raise SystemExit("BQ_DATASET must be set via env or --bq-dataset when using --to-bq")
                write_rows_to_bigquery(
                    rows,
                    dataset=bq_dataset,
                    table=table_for_channel,
                    project_id=bq_project,
                    credentials_file=bq_credentials,
                )
            else:
                for r in rows_country:
                    writer.writerow([
                        r.date,
                        app,
                        r.country_code,
                        f"{r.spend:.6f}",
                        getattr(r, "currency", ""),
                        getattr(r, "campaign_id", ""),
                        getattr(r, "campaign_name", ""),
                        getattr(r, "adgroup_id", ""),
                        getattr(r, "adgroup_name", ""),
                    ])

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

