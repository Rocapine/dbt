import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import requests

# Reuse the shared result type for uniform downstream handling (BQ writer, CSV output)
from tiktok_service import DailyCountrySpendPerAdGroup, _resolve_month_window


APPLE_ID_TOKEN_URL = "https://appleid.apple.com/auth/oauth2/token"
ASA_API_BASE_URL = "https://api.searchads.apple.com/api/v5"


def _script_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _client_secret_path() -> str:
    return os.path.join(_script_dir(), "client_secret.txt")


def _ensure_client_secret(*, python_executable: Optional[str] = None) -> str:
    """Ensure a fresh client_secret.txt exists by invoking asa_auth.py.

    Returns absolute path to client_secret.txt.
    """
    secret_path = _client_secret_path()
    # Always regenerate to avoid expiry issues (JWT can live up to 180 days, but cheap to regenerate)
    py = python_executable or os.getenv("PYTHON", "python3")
    auth_script = os.path.join(_script_dir(), "asa_auth.py")
    subprocess.run([py, auth_script], cwd=_script_dir(), check=True)
    if not os.path.isfile(secret_path):
        raise RuntimeError("Failed to generate Apple client secret (client_secret.txt not found)")
    return secret_path


def _get_access_token(*, client_id: Optional[str] = None, scope: str = "searchadsorg") -> str:
    """Get Apple Search Ads API access token.

    Order of precedence:
    1) ASA_ACCESS_TOKEN env var (use as-is)
    2) Generate client_secret via asa_auth.py, then exchange for access_token
    """
    token = os.getenv("ASA_ACCESS_TOKEN", "").strip()
    if token:
        return token

    # Exchange client_secret for access_token
    client_secret_file = _ensure_client_secret()
    with open(client_secret_file, "rt", encoding="utf-8") as f:
        client_secret = f.read().strip()

    resolved_client_id = client_id or os.getenv("ASA_CLIENT_ID", "").strip()
    if not resolved_client_id:
        # Fallback to the value used by asa_auth.py if present there
        # Note: asa_auth.py has client_id hardcoded
        resolved_client_id = "SEARCHADS.249553e6-77cd-403e-92dc-2e9e3d4e7467"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": resolved_client_id,
        "client_secret": client_secret,
        "scope": scope,
    }
    resp = requests.post(APPLE_ID_TOKEN_URL, headers=headers, data=data, timeout=30)
    resp.raise_for_status()
    payload = resp.json() or {}
    access_token = payload.get("access_token", "")
    if not access_token:
        raise RuntimeError(f"Apple token exchange failed: {payload}")
    return access_token


def _asa_headers(access_token: str, org_id: str) -> Dict[str, str]:
    return {
        "authorization": f"Bearer {access_token}",
        "content-type": "application/json",
        "x-ap-context": f"orgId={org_id}",
    }


def _fetch_campaign_summaries(session: requests.Session, headers: Dict[str, str], *, start_date: str, end_date: str) -> List[Dict[str, object]]:
    """Fetch campaign summaries via the reports endpoint to get id and name.

    The user-provided example shows POST /reports/campaigns returning an array of campaign objects
    with fields like id, name, countriesOrRegions, etc. We'll follow that structure here.
    """
    url = f"{ASA_API_BASE_URL}/campaigns/find"
    payload = {
        "conditions": [],
        "orderBy": [{"field": "modificationTime", "sortOrder": "DESCENDING"}],
        "pagination": {"offset": 0, "limit": 1000},
    }
    resp = session.post(url, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json() or {}
    campaigns = data.get("data", []) if isinstance(data, dict) else []
    # Normalize to list of dicts with id, name; keep other fields if present
    results: List[Dict[str, object]] = []
    for c in campaigns:
        if not isinstance(c, dict):
            continue
        cid = c.get("id")
        name = c.get("name")
        if cid is None or name is None:
            continue
        results.append(c)
    return results


def _fetch_campaign_details(session: requests.Session, headers: Dict[str, str], campaign_id: int) -> Dict[str, object]:
    url = f"{ASA_API_BASE_URL}/campaigns/{campaign_id}"
    resp = session.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json() or {}
    return data.get("data", {}) if isinstance(data, dict) else {}


def _fetch_campaign_adgroups_report(
    session: requests.Session,
    headers: Dict[str, str],
    *,
    campaign_id: int,
    start_date: str,
    end_date: str,
) -> List[Dict[str, object]]:
    """Fetch the adgroups report for a given campaign, daily granularity.

    Returns list of rows as in data.reportingDataResponse.row (each row has metadata and granularity[]).
    """
    url = f"{ASA_API_BASE_URL}/reports/campaigns/{campaign_id}/adgroups"
    payload = {
        "startTime": start_date,
        "endTime": end_date,
        "timeZone": "UTC",
        "granularity": "DAILY",
        "returnGrandTotals": False,
        "returnRowTotals": False,
        "returnRecordsWithNoMetrics": False,
        "selector": {
            "conditions": [],
            "orderBy": [{"field": "localSpend", "sortOrder": "DESCENDING"}],
            "pagination": {"offset": 0, "limit": 1000},
        },
    }
    resp = session.post(url, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json() or {}
    rows = (
        data.get("data", {})
        .get("reportingDataResponse", {})
        .get("row", [])
        if isinstance(data, dict)
        else []
    )
    return rows if isinstance(rows, list) else []


def fetch_daily_spend_by_country_by_adgroup(
    org_ids: Iterable[str],
    month_yyyy_mm: Optional[str] = None,
    start_date_str: Optional[str] = None,
    end_date_str: Optional[str] = None,
    timeout_seconds: int = 60,
) -> List[DailyCountrySpendPerAdGroup]:
    """Apple Search Ads: per-day spend attributed to campaign country per adgroup.

    Notes:
    - Country is derived from campaign details (countriesOrRegions). If multiple countries are present,
      country will be set to "MULTI".
    - Currency is taken from localSpend.currency per daily granularity entry.
    """
    if start_date_str and end_date_str:
        start_date, end_date = start_date_str, end_date_str
    else:
        start_date, end_date = _resolve_month_window(month_yyyy_mm)

    access_token = _get_access_token()
    results: List[DailyCountrySpendPerAdGroup] = []

    with requests.Session() as session:
        for org_id in org_ids:
            if not org_id:
                continue
            headers = _asa_headers(access_token, org_id)

            # Step 1: campaigns list (id, name)
            campaigns = _fetch_campaign_summaries(session, headers, start_date=start_date, end_date=end_date)
            campaign_id_to_name: Dict[int, str] = {}
            for c in campaigns:
                try:
                    cid = int(c.get("id"))
                except Exception:
                    continue
                campaign_id_to_name[cid] = str(c.get("name", ""))

            # Step 2: for each campaign get countries
            campaign_id_to_country: Dict[int, str] = {}
            for cid in campaign_id_to_name.keys():
                try:
                    details = _fetch_campaign_details(session, headers, cid)
                except Exception:
                    details = {}
                countries = details.get("countriesOrRegions") or []
                country_value: str
                if isinstance(countries, list) and len(countries) == 1:
                    country_value = str(countries[0])
                elif isinstance(countries, list) and len(countries) > 1:
                    country_value = "MULTI"
                else:
                    country_value = ""
                campaign_id_to_country[cid] = country_value

            # Step 3: adgroups report per campaign
            for cid, cname in campaign_id_to_name.items():
                try:
                    rows = _fetch_campaign_adgroups_report(
                        session,
                        headers,
                        campaign_id=cid,
                        start_date=start_date,
                        end_date=end_date,
                    )
                except Exception:
                    continue
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    meta = row.get("metadata", {}) or {}
                    adgroup_id = str(meta.get("adGroupId", "") or "")
                    adgroup_name = str(meta.get("adGroupName", "") or "")
                    gran = row.get("granularity", []) or []
                    for g in gran:
                        if not isinstance(g, dict):
                            continue
                        date_str = str(g.get("date", "") or "")
                        # Ensure YYYY-MM-DD
                        if len(date_str) >= 10:
                            date_str = date_str[:10]
                        spend_amount = 0.0
                        currency = ""
                        local_spend = g.get("localSpend") or {}
                        if isinstance(local_spend, dict):
                            amt = local_spend.get("amount")
                            cur = local_spend.get("currency")
                            try:
                                spend_amount = float(str(amt)) if amt is not None else 0.0
                            except Exception:
                                spend_amount = 0.0
                            currency = str(cur or "")
                        country_code = campaign_id_to_country.get(cid, "")
                        results.append(
                            DailyCountrySpendPerAdGroup(
                                date=date_str,
                                country_code=country_code,
                                spend=spend_amount,
                                currency=currency,
                                adgroup_id=adgroup_id,
                                adgroup_name=adgroup_name,
                                campaign_id=str(cid),
                                campaign_name=cname,
                            )
                        )

    return results



