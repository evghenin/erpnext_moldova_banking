from __future__ import annotations

import json
from datetime import datetime, date as date_cls, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import requests
import xml.etree.ElementTree as ET

import frappe
from frappe import _


BNM_URL = "https://www.bnm.md/en/official_exchange_rates"
# Official BNM rate (bnmRate) republished by Victoriabank. Different network path than www.bnm.md.
VICTORIABANK_RATES_URL = "https://www.victoriabank.md/bff/api/currency-rates"
VICTORIABANK_MARKET_TYPE = "14"
# (connect, read). A blackholed route must not hold a web worker for the proxy timeout.
BNM_TIMEOUT = (5, 10)

CACHE_TTL_SECONDS = 24 * 60 * 60  # 24 hours
CACHE_KEYS_LIST = "bnm:rates:keys:v1"
CACHE_PREFIX = "bnm:rates:v1"  # final key: bnm:rates:v1:<DD.MM.YYYY>


def _to_bnm_date_str(dt: date_cls) -> str:
    return dt.strftime("%d.%m.%Y")


def _parse_decimal(value: str) -> Decimal:
    v = (value or "").strip().replace(",", ".")
    try:
        return Decimal(v)
    except (InvalidOperation, ValueError) as e:
        raise frappe.ValidationError(_("Invalid numeric rate value: {0}").format(value)) from e


def _fetch_bnm_rates(dt: date_cls) -> Dict[str, Decimal]:
    """Fetch official BNM rates for a date. Empty dict if BNM has not published that date yet."""
    params = {"get_xml": "1", "date": _to_bnm_date_str(dt)}
    resp = requests.get(BNM_URL, params=params, timeout=BNM_TIMEOUT)
    resp.raise_for_status()

    body = (resp.text or "").strip()
    if not body:
        return {}

    try:
        root = ET.fromstring(body)
    except ET.ParseError as e:
        raise frappe.ValidationError(_("BNM returned invalid XML.")) from e

    rates: Dict[str, Decimal] = {}

    # Expected structure: <Valute><CharCode>EUR</CharCode><Nominal>1</Nominal><Value>...</Value></Valute>
    for valute in root.findall(".//Valute"):
        code = (valute.findtext("CharCode") or "").strip().upper()
        value_text = (valute.findtext("Value") or "").strip()
        nominal_text = (valute.findtext("Nominal") or "1").strip()

        if not code or not value_text:
            continue

        value = _parse_decimal(value_text)
        nominal = _parse_decimal(nominal_text)

        if nominal != 0:
            value = value / nominal

        rates[code] = value

    return rates


def _cache_get(key: str) -> Optional[Any]:
    cache = frappe.cache()
    raw = cache.get_value(key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _cache_set(key: str, payload: Any) -> None:
    cache = frappe.cache()
    cache.set_value(key, json.dumps(payload, ensure_ascii=False), expires_in_sec=CACHE_TTL_SECONDS)


def _keys_list_get() -> list:
    data = _cache_get(CACHE_KEYS_LIST)
    return data if isinstance(data, list) else []


def _keys_list_push_and_trim(new_key: str, limit: int = 10) -> None:
    keys = _keys_list_get()

    keys = [k for k in keys if k != new_key]
    keys.insert(0, new_key)

    evicted = keys[limit:]
    keys = keys[:limit]

    cache = frappe.cache()
    for k in evicted:
        cache.delete_value(k)

    _cache_set(CACHE_KEYS_LIST, keys)


def _rates_from_cache_payload(cached: Any) -> Optional[Dict[str, Decimal]]:
    if not isinstance(cached, dict) or not isinstance(cached.get("rates"), dict) or not cached["rates"]:
        return None
    out: Dict[str, Decimal] = {}
    for k, v in cached["rates"].items():
        out[str(k).upper()] = _parse_decimal(str(v))
    return out


def get_bnm_rates_cached(dt: date_cls, lookback_days: int = 10) -> Dict[str, Decimal]:
    """
    Fetch BNM rates with MRU cache of last 10 dates.
    Returns dict like {"EUR": Decimal("19.12"), ...} representing: 1 CUR = X MDL.

    If BNM has not published the requested date yet (weekends, holidays, or a
    future/transaction date), walk back to the latest published session.
    """
    last_error: Optional[Exception] = None

    for offset in range(max(0, lookback_days) + 1):
        candidate = dt - timedelta(days=offset)
        bnm_date = _to_bnm_date_str(candidate)
        cache_key = f"{CACHE_PREFIX}:{bnm_date}"

        cached_rates = _rates_from_cache_payload(_cache_get(cache_key))
        if cached_rates:
            _keys_list_push_and_trim(cache_key, limit=10)
            return cached_rates

        try:
            rates = _fetch_bnm_rates(candidate)
        except (requests.Timeout, requests.ConnectionError) as exc:
            # Unpublished dates come back as an empty body, not a dropped connection.
            # Retrying those network failures across the lookback window pins the worker.
            if not use_victoriabank_if_bnm_unavailable():
                raise
            rates = _victoriabank_bnm_rates(dt, lookback_days)
            if not rates:
                raise exc
            bnm_date = _to_bnm_date_str(dt)
            cache_key = f"{CACHE_PREFIX}:{bnm_date}"
        except Exception as e:
            last_error = e
            continue

        if not rates:
            continue

        payload = {
            "date": bnm_date,
            "rates": {k: str(v) for k, v in rates.items()},
            "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        }
        _cache_set(cache_key, payload)
        _keys_list_push_and_trim(cache_key, limit=10)
        return rates

    if last_error:
        raise last_error
    raise frappe.ValidationError(_("No currency rates found in BNM XML response."))


def _parse_victoriabank_bnm_rates(payload: Any) -> Dict[date_cls, Dict[str, Decimal]]:
    """Map calendar date -> {currency: MDL per 1 unit} using only bnmRate."""
    by_date: Dict[date_cls, Dict[str, Decimal]] = {}
    blocks = payload.get("rates") if isinstance(payload, dict) else None
    if not isinstance(blocks, list):
        return by_date

    for block in blocks:
        if not isinstance(block, dict):
            continue
        raw_date = str(block.get("fromDate") or "")[:10]
        try:
            day = datetime.strptime(raw_date, "%Y-%m-%d").date()
        except ValueError:
            continue
        rates = by_date.setdefault(day, {})
        for currency in block.get("currencies") or []:
            if not isinstance(currency, dict):
                continue
            code = str(currency.get("currency") or "").strip().upper()
            rows = currency.get("currencyRates") or []
            if not code or not isinstance(rows, list) or not rows:
                continue
            row = rows[-1] if isinstance(rows[-1], dict) else None
            if not row or row.get("bnmRate") in (None, ""):
                continue
            value = _parse_decimal(str(row.get("bnmRate")))
            nominal = _parse_decimal(str(row.get("nominal") or "1"))
            if nominal != 0:
                value = value / nominal
            rates[code] = value
    return by_date


def _victoriabank_bnm_rates(dt: date_cls, lookback_days: int) -> Dict[str, Decimal]:
    """Latest National Bank rates on or before dt, from one Victoriabank request."""
    start = dt - timedelta(days=max(0, lookback_days))
    resp = requests.get(
        VICTORIABANK_RATES_URL,
        params={
            "dateFrom": start.isoformat(),
            "dateTo": dt.isoformat(),
            "marketType": VICTORIABANK_MARKET_TYPE,
            "lang": "ro-RO",
        },
        timeout=BNM_TIMEOUT,
        headers={"Accept": "application/json", "User-Agent": "erpnext-moldova-banking"},
    )
    resp.raise_for_status()
    by_date = _parse_victoriabank_bnm_rates(resp.json())
    for offset in range(max(0, lookback_days) + 1):
        rates = by_date.get(dt - timedelta(days=offset))
        if rates:
            return rates
    return {}


def use_victoriabank_if_bnm_unavailable() -> bool:
    try:
        return bool(
            int(frappe.db.get_single_value("Moldova Banking Settings", "use_victoriabank_if_bnm_unavailable") or 0)
        )
    except Exception:
        return False


def bnm_exchange_rates_disabled() -> bool:
    """True when Moldova Banking Settings forbids any contact with www.bnm.md."""
    try:
        return bool(int(frappe.db.get_single_value("Moldova Banking Settings", "disable_bnm_exchange_rates") or 0))
    except Exception:
        return False


def _require_bnm_key(provided_key: str) -> None:
    settings = frappe.get_single("Moldova Banking Settings")
    expected = (settings.get("bnm_rates_key") or "").strip()

    if not expected:
        frappe.throw(_("BNM key is not configured."), frappe.PermissionError)

    provided = (provided_key or "").strip()
    if provided != expected:
        frappe.throw(_("Invalid key."), frappe.PermissionError)


def _calc_rate_via_mdl(rates: Dict[str, Decimal], from_currency: str, to_currency: str) -> float:
    fc = (from_currency or "").upper().strip()
    tc = (to_currency or "").upper().strip()

    if fc == tc:
        return 1.0

    # Interpret BNM feed as: 1 CUR = X MDL
    if fc == "MDL" and tc != "MDL":
        if tc not in rates:
            frappe.throw(_("BNM rate not found for {0}.").format(tc))
        return float(Decimal("1") / rates[tc])

    if tc == "MDL" and fc != "MDL":
        if fc not in rates:
            frappe.throw(_("BNM rate not found for {0}.").format(fc))
        return float(rates[fc])

    if fc not in rates or tc not in rates:
        frappe.throw(_("BNM rate not found for {0} or {1}.").format(fc, tc))
    return float(rates[fc] / rates[tc])


@frappe.whitelist(allow_guest=True)
def get_exchange_rate(
    from_currency: Optional[str] = None,
    to_currency: Optional[str] = None,
    date: Optional[str] = None,
    key: Optional[str] = None,
    api_key: Optional[str] = None,
):
    from frappe.utils import getdate

    _require_bnm_key(api_key or key)

    if bnm_exchange_rates_disabled():
        frappe.throw(_("BNM exchange rates are disabled in Moldova Banking Settings."), frappe.ValidationError)

    if not date or not from_currency or not to_currency:
        frappe.throw(_("Missing required parameters: date, from_currency, to_currency, key"), frappe.ValidationError)

    try:
        dt = getdate(date)
    except Exception:
        frappe.throw(_("Invalid date format. Expected YYYY-MM-DD."), frappe.ValidationError)

    rates = get_bnm_rates_cached(dt)
    rate = _calc_rate_via_mdl(rates, from_currency, to_currency)

    # Must match result_key = "result"
    return {"result": rate}
