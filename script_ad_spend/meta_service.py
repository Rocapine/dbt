import os
from typing import Dict, Iterable, List, Optional
import logging as console
import requests

from tiktok_service import DailyCountrySpendPerAdGroup, _resolve_month_window  


GRAPH_BASE_URL = "https://graph.facebook.com/v23.0"


def fetch_daily_spend_by_country(ad_account_ids: Iterable[str],month_yyyy_mm: Optional[str] = None,start_date_str: Optional[str] = None,
    end_date_str: Optional[str] = None,
    timeout_seconds: int = 30,
) -> List[DailyCountrySpendPerAdGroup]:

    token = "EAAUKh5VZA6xgBPl45ALNAMJdMoX53w4tW60i2FwCnE2A20YmlD4ruuvJnuhM76rkvi8MyBDivE57v52guCZCvBqnpZAfZBO249wOoOAIlgqgpFtMlZBBuu3gow126AreW2s1wwOhMQYyCwqrMh0nDkavdlEqK60neojGc1I9AGxdbitwTyHkYlzxngTZCP"

    if start_date_str and end_date_str:
        start_date, end_date = start_date_str, end_date_str
    else:
        start_date, end_date = _resolve_month_window(month_yyyy_mm)

    session = requests.Session()

    results: List[DailyCountrySpendPerAdGroup] = []

    for account_id in ad_account_ids:
        # Fetch account currency by calling the ad account endpoint
        account_currency: str = ""
        try:
            acct_resp = session.request(
                "GET",
                f"{GRAPH_BASE_URL}/act_{account_id}",
                params={
                    "access_token": token,
                    "fields": "id,account_id,name,currency",
                },
                timeout=timeout_seconds,
            )
            acct_resp.raise_for_status()
            acct_data = acct_resp.json() or {}
            account_currency = str(acct_data.get("currency") or "")
            console.info(f"Account currency: {account_currency}")
        except Exception:
            account_currency = ""
        # Initial request URL with query params
        url = f"{GRAPH_BASE_URL}/act_{account_id}/insights"
        params = {
            "access_token": token,
            "fields": "spend,date_start,date_stop,adset_id,adset_name,campaign_id,campaign_name",
            "breakdowns": "country",
            "time_increment": "1",
            "level": "adset",
            "time_range[since]": start_date,
            "time_range[until]": end_date,
            "limit": 500,
        }
        # Pagination variables
        next_url: Optional[str] = url
        next_params: Optional[Dict[str, str]] = params

        while next_url is not None:
            resp = session.request("GET", next_url, params=next_params, timeout=timeout_seconds)
            resp.raise_for_status()
            data = resp.json() or {}

            list_rows = data.get("data", []) if isinstance(data, dict) else []
            for row in list_rows:
                date_str = (row.get("date_start"))[:10]
                country = row.get("country")
                spend_str = row.get("spend") 
                spend_val = float(spend_str)
                adset_id = str(row.get("adset_id"))
                adset_name = str(row.get("adset_name"))
                campaign_id = str(row.get("campaign_id"))
                campaign_name = str(row.get("campaign_name"))

                results.append(
                    DailyCountrySpendPerAdGroup(
                        date=date_str,
                        country_code=country,
                        spend=spend_val,
                        currency=account_currency,
                        adgroup_id=adset_id,
                        adgroup_name=adset_name,
                        campaign_id=campaign_id,
                        campaign_name=campaign_name,
                    )
                )

            # Pagination via paging.next
            paging = data.get("paging", {}) if isinstance(data, dict) else {}
            next_link = paging.get("next")
            if next_link:
                next_url = next_link
                next_params = None
            else:
                next_url = None
                next_params = None

    return results


