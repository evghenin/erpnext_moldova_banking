from __future__ import annotations

import json
from datetime import datetime, date as date_cls
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import requests
import xml.etree.ElementTree as ET

import frappe
from frappe import _


BNM_URL = "https://www.bnm.md/en/official_exchange_rates"
BNM_TIMEOUT = 30

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
    params = {"get_xml": "1", "date": _to_bnm_date_str(dt)}
    resp = requests.get(BNM_URL, params=params, timeout=BNM_TIMEOUT)
    resp.raise_for_status()

    try:
        root = ET.fromstring(resp.text)
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

    if not rates:
        raise frappe.ValidationError(_("No currency rates found in BNM XML response."))

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


def get_bnm_rates_cached(dt: date_cls) -> Dict[str, Decimal]:
    """
    Fetch BNM rates with MRU cache of last 10 dates.
    Returns dict like {"EUR": Decimal("19.12"), ...} representing: 1 CUR = X MDL.
    """
    bnm_date = _to_bnm_date_str(dt)
    cache_key = f"{CACHE_PREFIX}:{bnm_date}"

    cached = _cache_get(cache_key)
    if isinstance(cached, dict) and isinstance(cached.get("rates"), dict):
        _keys_list_push_and_trim(cache_key, limit=10)
        rates_raw = cached["rates"]
        out: Dict[str, Decimal] = {}
        for k, v in rates_raw.items():
            out[str(k).upper()] = _parse_decimal(str(v))
        return out

    rates = _fetch_bnm_rates(dt)
    payload = {
        "date": bnm_date,
        "rates": {k: str(v) for k, v in rates.items()},
        "fetched_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

    _cache_set(cache_key, payload)
    _keys_list_push_and_trim(cache_key, limit=10)
    return rates


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
    key: Optional[str] = None
):
    _require_bnm_key(key)

    if not date or not from_currency or not to_currency:
        frappe.throw(_("Missing required parameters: date, from_currency, to_currency, key"), frappe.ValidationError)

    try:
        dt = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        frappe.throw(_("Invalid date format. Expected YYYY-MM-DD."), frappe.ValidationError)

    rates = get_bnm_rates_cached(dt)
    rate = _calc_rate_via_mdl(rates, from_currency, to_currency)

    # Must match result_key = "result"
    return {"result": rate}
