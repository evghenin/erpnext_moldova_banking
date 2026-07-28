# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from __future__ import annotations

from typing import Any

import requests

import frappe
from frappe import _
from frappe.utils.password import get_decrypted_password

SETTINGS_DOCTYPE = "Moldova Banking Settings"

MAIB_DEFAULTS = {
	"Test": {
		"token_url": "https://test-business-sso.maib.md/api/connect/token",
		"api_base_url": "https://test-bb-transfers-gateway.maib.md",
		"scope": "payments_gateway",
	},
	"Production": {
		"token_url": "https://business-sso.maib.md/api/connect/token",
		"api_base_url": "https://api-gateway.maib.md",
		"scope": "payments_gateway",
	},
}

STATEMENT_PATH = "/api/account-statement-queries"
BALANCE_PATH = "/api/account-balance-queries"


def get_maib_settings():
	return frappe.get_single(SETTINGS_DOCTYPE)


def get_maib_defaults(environment: str | None = None) -> dict[str, str]:
	return dict(MAIB_DEFAULTS.get(environment or "Test") or {})


def resolve_maib_endpoints(settings=None) -> dict[str, str]:
	settings = settings or get_maib_settings()
	defaults = get_maib_defaults(settings.maib_environment or "Test")
	return {
		"token_url": (settings.maib_token_url or "").strip() or defaults.get("token_url") or "",
		"api_base_url": (settings.maib_api_base_url or "").strip() or defaults.get("api_base_url") or "",
		"scope": (settings.maib_scope or "").strip() or defaults.get("scope") or "payments_gateway",
	}


def get_maib_client_secret(settings=None) -> str:
	settings = settings or get_maib_settings()
	try:
		return (
			get_decrypted_password(SETTINGS_DOCTYPE, SETTINGS_DOCTYPE, "maib_client_secret", raise_exception=False)
			or ""
		)
	except Exception:
		return ""


def iban_to_maib_account_id(iban: str | None) -> str:
	"""Derive MAIB numeric account id from a Moldovan IBAN (MD + check + bank + account)."""
	cleaned = (iban or "").replace(" ", "").upper()
	if cleaned.startswith("MD") and len(cleaned) > 8:
		return cleaned[8:].lstrip("0") or "0"
	digits = "".join(ch for ch in cleaned if ch.isdigit())
	return digits.lstrip("0") or digits


def resolve_api_account_id(bank_account: str) -> str:
	iban = frappe.db.get_value("Bank Account", bank_account, "iban")
	account_id = iban_to_maib_account_id(iban)
	if not account_id:
		frappe.throw(_("Bank Account {0} has no IBAN.").format(bank_account))
	return account_id


def get_access_token(settings=None) -> dict[str, Any]:
	settings = settings or get_maib_settings()
	if not settings.maib_enabled:
		frappe.throw(_("MAIB API is disabled in Moldova Banking Settings."))

	client_id = (settings.maib_client_id or "").strip()
	client_secret = get_maib_client_secret(settings)
	endpoints = resolve_maib_endpoints(settings)

	if not client_id or not client_secret:
		frappe.throw(_("MAIB Client ID and Client Secret are required."))
	if not endpoints["token_url"]:
		frappe.throw(_("MAIB Token URL is required."))

	try:
		response = requests.post(
			endpoints["token_url"],
			data={
				"grant_type": "client_credentials",
				"client_id": client_id,
				"client_secret": client_secret,
				"scope": endpoints["scope"],
			},
			headers={"Content-Type": "application/x-www-form-urlencoded"},
			timeout=30,
		)
	except requests.RequestException as e:
		frappe.throw(_("Could not reach MAIB token endpoint: {0}").format(str(e)))

	if response.status_code >= 400:
		frappe.throw(
			_("MAIB token request failed ({0}): {1}").format(response.status_code, response.text[:500])
		)

	try:
		payload = response.json()
	except Exception:
		frappe.throw(_("MAIB token endpoint returned non-JSON response."))

	if not payload.get("access_token"):
		frappe.throw(_("MAIB token response did not include access_token."))

	payload["_endpoints"] = endpoints
	return payload


def _xml_headers(token: str) -> dict[str, str]:
	return {
		"Content-Type": "application/xml",
		"Accept": "application/xml",
		"Authorization": f"Bearer {token}",
	}


def build_statement_request_xml(account_id: str, from_date: str, to_date: str, product: str = "Operational") -> str:
	"""from_date/to_date are yyyyMMdd strings."""
	return (
		'<?xml version="1.0" encoding="UTF-8"?>'
		"<root>"
		f"<{product}><Account>{frappe.as_unicode(account_id)}</Account></{product}>"
		f"<Date><From>{from_date}</From><To>{to_date}</To></Date>"
		"<ChargeAgreed>Y</ChargeAgreed>"
		"</root>"
	)


def fetch_statement_xml(
	account_id: str,
	from_yyyymmdd: str,
	to_yyyymmdd: str,
	settings=None,
	product: str = "Operational",
) -> str:
	settings = settings or get_maib_settings()
	token_payload = get_access_token(settings)
	token = token_payload["access_token"]
	base = (token_payload.get("_endpoints") or resolve_maib_endpoints(settings))["api_base_url"].rstrip("/")
	if not base:
		frappe.throw(_("MAIB API Base URL is required."))

	url = f"{base}{STATEMENT_PATH}"
	body = build_statement_request_xml(account_id, from_yyyymmdd, to_yyyymmdd, product=product)

	try:
		response = requests.post(url, data=body.encode("utf-8"), headers=_xml_headers(token), timeout=120)
	except requests.RequestException as e:
		frappe.throw(_("Could not reach MAIB statement endpoint: {0}").format(str(e)))

	if response.status_code >= 400:
		frappe.throw(
			_("MAIB statement request failed ({0}): {1}").format(response.status_code, response.text[:800])
		)

	return response.text or ""
