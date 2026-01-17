from __future__ import annotations

import json
import secrets
from typing import Any, Dict
from urllib.parse import urlparse

import frappe

SETTINGS_DOCTYPE = "Moldova Banking Settings"
CURRENCY_EXCHANGE_SETTINGS_DOCTYPE = "Currency Exchange Settings"

# Path-only endpoint (no scheme/host/port) expected in Currency Exchange Settings when using Custom provider.
BNM_METHOD_PATH = "/api/method/erpnext_moldova_banking.api.bnm_rates.get_exchange_rate"

DEFAULT_KEY_LENGTH = 48


def _generate_key(length: int = DEFAULT_KEY_LENGTH) -> str:
    """Generate a URL-safe random key."""
    token = secrets.token_urlsafe(max(32, length))
    return token[:length]


def _normalize_path(endpoint: str) -> str:
    """
    Accept either a full URL (https://domain/...),
    or a path-only endpoint (/api/method/...)
    and normalize to path-only.
    """
    endpoint = (endpoint or "").strip()
    if not endpoint:
        return ""

    if "://" in endpoint:
        parsed = urlparse(endpoint)
        return (parsed.path or "").strip()

    return endpoint


def ensure_bnm_rates_key_and_sync_currency_exchange_settings(force_regen: bool = False) -> str:
    """
    Ensure Moldova Banking Settings has bnm_rates_key. If missing, generate and save.
    Optionally force regeneration.
    If Currency Exchange Settings is set to Custom and points to our endpoint path, update/add req_params.key.
    Returns the active key.
    """
    settings = frappe.get_single(SETTINGS_DOCTYPE)

    current_key = (settings.get("bnm_rates_key") or "").strip()
    if force_regen or not current_key:
        current_key = _generate_key()
        # For Single settings, read-only fields may not persist via doc.save().
        frappe.db.set_single_value(SETTINGS_DOCTYPE, "bnm_rates_key", current_key)
        frappe.db.commit()

    _sync_currency_exchange_settings_key(current_key)
    return current_key


def _sync_currency_exchange_settings_key(key: str) -> None:
    """
    If Currency Exchange Settings uses Custom provider and endpoint path matches our method,
    ensure req_params (child table: Currency Exchange Settings Details) contains key=<key>.
    """
    try:
        ces = frappe.get_single(CURRENCY_EXCHANGE_SETTINGS_DOCTYPE)
    except Exception:
        return

    service_provider = (ces.get("service_provider") or ces.get("exchange_rate_provider") or "").strip()
    if service_provider.lower() != "custom":
        return

    api_endpoint = (
        ces.get("api_endpoint")
        or ""
    ).strip()

    endpoint_path = _normalize_path(api_endpoint)
    if endpoint_path != BNM_METHOD_PATH:
        return

    rows = ces.get("req_params") or []

    # req_params row fields are: key, value
    found = None
    for r in rows:
        if (r.get("key") or "").strip().lower() == "key":
            found = r
            break

    if found:
        found.set("value", key)
    else:
        ces.append("req_params", {"key": "key", "value": key})

    ces.save(ignore_permissions=True)


@frappe.whitelist()
def regenerate_bnm_rates_key() -> str:
    """
    Regenerate BNM key and sync Currency Exchange Settings req_params.key if applicable.
    Intended to be called from a Button field in Moldova Banking Settings.
    """
    user = frappe.session.user
    if user == "Guest":
        frappe.throw("Not permitted.", frappe.PermissionError)

    roles = set(frappe.get_roles(user) or [])
    if "System Manager" not in roles:
        frappe.throw("Not permitted.", frappe.PermissionError)

    return ensure_bnm_rates_key_and_sync_currency_exchange_settings(force_regen=True)

@frappe.whitelist()
def configure_currency_exchange_bnm() -> str:
    user = frappe.session.user
    if user == "Guest":
        frappe.throw("Not permitted.", frappe.PermissionError)

    roles = set(frappe.get_roles(user) or [])
    if "System Manager" not in roles:
        frappe.throw("Not permitted.", frappe.PermissionError)

    key = ensure_bnm_rates_key_and_sync_currency_exchange_settings(force_regen=False)

    ces = frappe.get_single(CURRENCY_EXCHANGE_SETTINGS_DOCTYPE)
    ces.set("service_provider", "Custom")

    from frappe.utils import get_url
    full_endpoint = get_url(BNM_METHOD_PATH)
    ces.set("api_endpoint", full_endpoint)

    ces.set("req_params", [])
    ces.append("req_params", {"key": "date", "value": "{transaction_date}"})
    ces.append("req_params", {"key": "from_currency", "value": "{from_currency}"})
    ces.append("req_params", {"key": "to_currency", "value": "{to_currency}"})
    ces.append("req_params", {"key": "key", "value": key})

    ces.set("result_key", [])
    ces.append("result_key", {"key": "message"})
    ces.append("result_key", {"key": "result"})

    ces.save(ignore_permissions=True)
    return "ok"