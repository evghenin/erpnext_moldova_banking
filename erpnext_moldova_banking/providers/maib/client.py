# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from __future__ import annotations

import json
from typing import Any

import requests

import frappe
from frappe import _
from frappe.utils.password import get_decrypted_password

SETTINGS_DOCTYPE = "Moldova Banking Settings"
LEGACY_TEST_TOKEN_URL = "https://test-business-sso.maib.md/api/connect/token"

MAIB_DEFAULTS = {
	"Test": {
		"token_url": "https://test-business-sso.maib.md/connect/token",
		"api_base_url": "https://test-bb-transfers-gateway.maib.md",
		"scope": "payments_gateway",
	},
	"Production": {
		"token_url": "https://business-sso.maib.md/connect/token",
		"api_base_url": "https://business-api.maib.md",
		"scope": "payments_gateway",
	},
}

STATEMENT_PATH = "/api/account-statement-queries"
BALANCE_PATH = "/api/account-balance-queries"


def get_maib_settings():
	"""Get global MAIB settings (environment, endpoints, enabled flag)."""
	return frappe.get_single(SETTINGS_DOCTYPE)


def _normalize_company_name(value: Any) -> Any:
	"""Normalize values coming from Frappe MultiSelect / JSON serialization."""
	if value is None:
		return ""
	if isinstance(value, (list, tuple, set)):
		normalized = [_normalize_company_name(item) for item in value]
		normalized = [item for item in normalized if item not in (None, "")]
		if len(normalized) == 1:
			return normalized[0]
		return normalized
	if isinstance(value, str):
		candidate = value.strip()
		if not candidate:
			return ""
		if candidate.startswith("[") and candidate.endswith("]"):
			try:
				loaded = json.loads(candidate)
				return _normalize_company_name(loaded)
			except Exception:
				pass
		if "," in candidate:
			parts = [part.strip() for part in candidate.split(",") if part.strip()]
			if len(parts) == 1:
				return parts[0]
			return parts
		return candidate
	return str(value).strip()


def get_company_maib_settings(company: str | None = None):
	"""Get company-specific MAIB credentials (client_id, client_secret), or None if not configured."""
	if not company:
		return None
	settings = frappe.get_single(SETTINGS_DOCTYPE)
	normalized_candidates = _normalize_company_name(company)
	if isinstance(normalized_candidates, list):
		candidate_set = {str(item).strip() for item in normalized_candidates if str(item).strip()}
	else:
		candidate_set = {str(normalized_candidates).strip()} if str(normalized_candidates).strip() else set()
	for row in settings.get("maib_company_settings") or []:
		row_company_raw = getattr(row, "company", "")
		if hasattr(row, "get"):
			row_company_raw = row.get("company", row_company_raw)
		row_company = _normalize_company_name(row_company_raw)
		row_values = row_company if isinstance(row_company, list) else [row_company]
		row_values = [str(item).strip() for item in row_values if str(item).strip()]
		if any(item in candidate_set for item in row_values):
			return row
	return None


def get_maib_defaults(environment: str | None = None) -> dict[str, str]:
	return dict(MAIB_DEFAULTS.get(environment or "Test") or {})


def resolve_maib_endpoints(settings=None) -> dict[str, str]:
	"""Resolve MAIB endpoints from global settings.

	Accepts either the actual DocType object or a dict-like settings payload for
	unit tests and compatibility callers.
	"""
	settings = settings or get_maib_settings()
	environment = (getattr(settings, "maib_environment", None) or settings.get("maib_environment") or "Test") if hasattr(settings, "get") else (getattr(settings, "maib_environment", None) or "Test")
	defaults = get_maib_defaults(environment)
	token_url = (getattr(settings, "maib_token_url", None) or settings.get("maib_token_url") or "").strip() if hasattr(settings, "get") else (getattr(settings, "maib_token_url", None) or "").strip()
	if environment == "Test" and token_url.rstrip("/") == LEGACY_TEST_TOKEN_URL:
		token_url = defaults.get("token_url") or ""
	api_base_url = (getattr(settings, "maib_api_base_url", None) or settings.get("maib_api_base_url") or "").strip() if hasattr(settings, "get") else (getattr(settings, "maib_api_base_url", None) or "").strip()
	scope = (getattr(settings, "maib_scope", None) or settings.get("maib_scope") or "").strip() if hasattr(settings, "get") else (getattr(settings, "maib_scope", None) or "").strip()
	return {
		"token_url": token_url or defaults.get("token_url") or "",
		"api_base_url": api_base_url or defaults.get("api_base_url") or "",
		"scope": scope or defaults.get("scope") or "payments_gateway",
	}


def get_maib_client_secret(company: str | None = None) -> str:
	"""Get company-specific client secret."""
	if company:
		company_row = get_company_maib_settings(company)
		if company_row:
			value = getattr(company_row, "client_secret", None) or ""
			# Password fields in child table rows may be encrypted; attempt decryption
			if value:
				try:
					from frappe.utils.password import get_decrypted_password
					# Try to get decrypted value from the child table row
					# For child tables, the row is typically an object with .name attribute
					if hasattr(company_row, "name"):
						decrypted = get_decrypted_password(
							"MAIB Company Setting",
							company_row.name,
							"client_secret",
							raise_exception=False
						)
						if decrypted:
							return str(decrypted)
				except Exception:
					pass
				# If not encrypted or decryption failed, use the value as-is
				if value:
					return str(value)
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


def get_access_token(company: str | None = None, settings=None) -> dict[str, Any]:
	"""Get MAIB access token using per-company credentials if available.

	For backward compatibility, also accepts a settings parameter (ignored if company is provided).
	"""
	# For backward compatibility with old code that passes settings directly
	if settings and not company:
		# If settings parameter is provided (legacy), extract company if available
		if hasattr(settings, "company"):
			company = getattr(settings, "company", None)
	company = _normalize_company_name(company)
	if isinstance(company, list):
		company = company[0] if company else None
	company = str(company).strip() if company is not None else None

	global_settings = get_maib_settings()
	if not getattr(global_settings, "maib_enabled", 0) in (True, 1, "1", "true"):
		frappe.throw(_("MAIB API is disabled in Moldova Banking Settings."))

	# Get company-specific credentials
	client_id = None
	client_secret = None

	if company:
		company_row = get_company_maib_settings(company)
		if company_row:
			# Try to access fields as attributes or dict keys
			client_id = getattr(company_row, "client_id", "")
			if not client_id and isinstance(company_row, dict):
				client_id = company_row.get("client_id", "")
			client_id = str(client_id or "").strip()
			client_secret = get_maib_client_secret(company)

	if not client_id or not client_secret:
		frappe.throw(_("MAIB credentials are not configured for company {0}.").format(company or "(unknown)"))

	endpoints = resolve_maib_endpoints()

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
	payload["_environment"] = global_settings.maib_environment or "Test"
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
	company: str | None = None,
	product: str = "Operational",
) -> str:
	token_payload = get_access_token(company=company)
	token = token_payload["access_token"]
	base = (token_payload.get("_endpoints") or resolve_maib_endpoints())["api_base_url"].rstrip("/")
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
