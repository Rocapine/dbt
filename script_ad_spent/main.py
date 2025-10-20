import os
import sys
import csv
from typing import List, Optional
from datetime import datetime, timedelta
from dotenv import load_dotenv
from constant import HISTORICAL_TIKTOK_IDS, HISTORICAL_META_IDS, NEW_AD_ACCOUNT_TIKTOK_IDS, NEW_AD_ACCOUNT_META_IDS
from tiktok_service import fetch_daily_spend_by_country_by_adgroup as tiktok_spend_by_country_by_adgroup
from meta_service import fetch_daily_spend_by_country as meta_spend_by_country
from bq_writer import write_rows_to_bigquery


def main(argv: List[str]) -> int:
    load_dotenv()

    args = argv[1:]
    TIKTOK_IDS = HISTORICAL_TIKTOK_IDS
    META_IDS = HISTORICAL_META_IDS

    # parsed date args
    start_date_arg: Optional[str] = None
    end_date_arg: Optional[str] = None

    # flags
    to_bq = False
    use_meta = False
    bq_dataset = os.getenv("BQ_DATASET")
    bq_table = os.getenv("BQ_TIKTOK_TABLE")
    bq_project = os.getenv("BQ_PROJECT")
    bq_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if "--to-bq" in args:
        to_bq = True
        args = [a for a in args if a != "--to-bq"]
    if "--new-ad-account" in args:
        args = [a for a in args if a != "--new-ad-account"]
        TIKTOK_IDS = NEW_AD_ACCOUNT_TIKTOK_IDS
        META_IDS = NEW_AD_ACCOUNT_META_IDS
    if "--meta" in args:
        use_meta = True
        args = [a for a in args if a != "--meta"]
        bq_table = os.getenv("BQ_META_TABLE")
    # Parse positional args for date range
    def is_yyyy_mm_dd(s: str) -> bool:
        try:
            datetime.strptime(s, "%Y-%m-%d")
            return True
        except Exception:
            return False

    parsed_apps_start_idx = 0
    if len(args) >= 2 and is_yyyy_mm_dd(args[0]) and is_yyyy_mm_dd(args[1]):
        start_date_arg, end_date_arg = args[0], args[1]
        parsed_apps_start_idx = 2
    else:
        # Default to yesterday (UTC) when dates are not supplied
        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date_arg = yesterday
        end_date_arg = yesterday

    # support selecting specific apps via CLI args after date inputs
    default_app_ids = META_IDS if use_meta else TIKTOK_IDS
    selected_apps = set(args[parsed_apps_start_idx:]) if len(args) > parsed_apps_start_idx else set(default_app_ids.keys())

    def advertiser_ids_for_app(app: str) -> List[str]:
        if not use_meta:
            return [TIKTOK_IDS.get(app, "")]
        return [META_IDS.get(app, "")]

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

    for app in sorted(selected_apps):
        ids = advertiser_ids_for_app(app)
        rows_country = (
            meta_spend_by_country if use_meta else tiktok_spend_by_country_by_adgroup
        )(ids, start_date_str=start_date_arg, end_date_str=end_date_arg)
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
                table=bq_table,
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

