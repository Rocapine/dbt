import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import requests


TIKTOK_REPORT_URL  =  "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
TIKTOK_ADGROUP_URL = "https://business-api.tiktok.com/open_api/v1.3/adgroup/get/"


@dataclass
class DailyCountrySpendPerAdGroup:
    date: str  # YYYY-MM-DD
    country_code: str  # e.g., US, FR
    spend: float  # in account currency as returned by API
    currency: str = ""
    adgroup_id: str = ""
    adgroup_name: str = ""
    campaign_id: str = ""
    campaign_name: str = ""


def _resolve_month_window(month_yyyy_mm: Optional[str]) -> Tuple[str, str]:
    """Resolve start_date and end_date (YYYY-MM-DD) for a given YYYY-MM month string.
    If month is None or invalid, use current month in UTC.
    """
    layout = "%Y-%m"
    start_date: datetime
    if month_yyyy_mm:
        try:
            t = datetime.strptime(month_yyyy_mm, layout).replace(tzinfo=timezone.utc)
            start_date = datetime(t.year, t.month, 1, tzinfo=timezone.utc)
        except ValueError:
            now = datetime.now(timezone.utc)
            start_date = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    else:
        now = datetime.now(timezone.utc)
        start_date = datetime(now.year, now.month, 1, tzinfo=timezone.utc)

    # last day of month: next month first day - 1 day
    if start_date.month == 12:
        next_month = datetime(start_date.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        next_month = datetime(start_date.year, start_date.month + 1, 1, tzinfo=timezone.utc)
    end_date_dt = next_month - timedelta(days=1)
    return start_date.strftime("%Y-%m-%d"), end_date_dt.strftime("%Y-%m-%d")


def fetch_daily_spend_by_country_by_adgroup(
    advertiser_ids: Iterable[str],
    month_yyyy_mm: Optional[str] = None,
    start_date_str: Optional[str] = None,
    end_date_str: Optional[str] = None,
    timeout_seconds: int = 30,
) -> List[DailyCountrySpendPerAdGroup]:
    """Fetch per-day, per-country spend for one or more TikTok advertiser accounts.

    - Reads token from env var TIKTOK_TOKEN
    - Aggregates across provided advertiser_ids
    - Returns raw spend values in the account currency reported by TikTok
    """
    token = "57ce2e64ea07ecb03836462d8322714ba925b523"


    if start_date_str and end_date_str:
        start_date, end_date = start_date_str, end_date_str
    else:
        start_date, end_date = _resolve_month_window(month_yyyy_mm)

    session = requests.Session()
    headers = {
        "Content-Type": "application/json",
        "Access-Token": token,
    }

    # We will collect raw rows per advertiser without aggregation
    results: List[DailyCountrySpendPerAdGroup] = []

    for advertiser_id in advertiser_ids:
        if not advertiser_id:
            continue
        payload = {
            "advertiser_id": advertiser_id,
            "start_date": start_date,
            "end_date": end_date,
            "metrics": ["spend", "currency"],
            "report_type": "BASIC",
            "data_level": "AUCTION_ADGROUP",
            "dimensions": ["stat_time_day", "country_code", "adgroup_id"],
            "page": 1,
            "page_size": 1000,
        }

        # Collect adgroup_ids and raw rows observed for this advertiser to resolve details later
        advertiser_adgroup_ids: set[str] = set()
        raw_rows: List[Tuple[str, str, str, float, str]] = []  # (date, country, adgroup_id, spend, currency)

        while True:
            resp = session.request("GET", TIKTOK_REPORT_URL, json=payload, headers=headers, timeout=timeout_seconds)
            resp.raise_for_status()
            data = resp.json()

            # Expect code == 0 on success 
            if data.get("code") != 0:
                message = data.get("message", "unknown error")
                raise RuntimeError(f"TikTok API error: {message}")

            list_rows = (
                data.get("data", {}).get("list", []) if isinstance(data.get("data"), dict) else []
            )

            for row in list_rows:
                dims = row.get("dimensions", {}) or {}
                metrics = row.get("metrics", {}) or {}
                raw_date = dims.get("stat_time_day") or dims.get("stat_time_day.0") or ""
                # Normalize to YYYY-MM-DD
                date_str = raw_date[:10] if isinstance(raw_date, str) and len(raw_date) >= 10 else str(raw_date)
                country = dims.get("country_code", "")
                adgroup_id = dims.get("adgroup_id", "") or ""
                spend_str = metrics.get("spend") or "0"
                currency = metrics.get("currency") or ""
                spend_val = float(spend_str)
                raw_rows.append((date_str, country, adgroup_id, spend_val, currency))
                if adgroup_id:
                    advertiser_adgroup_ids.add(adgroup_id)

            # We paginate if needed 
            page_info = data.get("data", {}).get("page_info", {}) if isinstance(data.get("data"), dict) else {}
            total_page = page_info.get("total_page")
            current_page = page_info.get("page") or payload["page"]
            if isinstance(total_page, int) and isinstance(current_page, int):
                if current_page >= total_page:
                    break
                payload["page"] = current_page + 1
                continue
            break

        # Resolve adgroup details for this advertiser in batches
        ad_details: Dict[str, Dict[str, str]] = {}
        if advertiser_adgroup_ids:
            batch_size = 50
            unresolved = list(advertiser_adgroup_ids)
            for i in range(0, len(unresolved), batch_size):
                batch = unresolved[i:i + batch_size]
                ad_payload = {
                    "advertiser_id": advertiser_id,
                    "filtering": {"adgroup_ids": batch},
                    "fields": [
                        "adgroup_id",
                        "adgroup_name",
                        "campaign_id",
                        "campaign_name",
                    ],
                    "page": 1,
                    "page_size": 1000,
                }
                while True:
                    resp = session.request("GET", TIKTOK_ADGROUP_URL, json=ad_payload, headers=headers, timeout=timeout_seconds)
                    resp.raise_for_status()
                    ad_data = resp.json() or {}
                    if ad_data.get("code") != 0:
                        break
                    alist = ad_data.get("data", {}).get("list", []) if isinstance(ad_data.get("data"), dict) else []
                    for a in alist:
                        agid = str(a.get("adgroup_id", "") or "")
                        if not agid:
                            continue
                        ad_details[agid] = {
                            "adgroup_id": str(a.get("adgroup_id", "") or ""),
                            "adgroup_name": str(a.get("adgroup_name", "") or ""),
                            "campaign_id": str(a.get("campaign_id", "") or ""),
                            "campaign_name": str(a.get("campaign_name", "") or ""),
                        }
                    page_info = ad_data.get("data", {}).get("page_info", {}) if isinstance(ad_data.get("data"), dict) else {}
                    total_page = page_info.get("total_page")
                    current_page = page_info.get("page") or ad_payload["page"]
                    if isinstance(total_page, int) and isinstance(current_page, int) and current_page < total_page:
                        ad_payload["page"] = current_page + 1
                        continue
                    break
        for date_str, country, adgroup_id, spend, currency in raw_rows:
            details = ad_details.get(adgroup_id, {})
            results.append(
                DailyCountrySpendPerAdGroup(
                    date=date_str,
                    country_code=country,
                    spend=spend,
                    currency=currency,
                    adgroup_id=adgroup_id or details.get("adgroup_id", ""),
                    adgroup_name=details.get("adgroup_name", ""),
                    campaign_id=details.get("campaign_id", ""),
                    campaign_name=details.get("campaign_name", ""),
                )
            )
    return results



