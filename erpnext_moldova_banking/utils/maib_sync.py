# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from __future__ import annotations

from typing import Any

import frappe
from frappe import _
from frappe.utils import add_days, get_datetime, getdate, now_datetime, today

from erpnext_moldova_banking.providers.maib.client import (
	fetch_statement_xml,
	get_access_token,
	get_maib_defaults,
	resolve_api_account_id,
	resolve_maib_endpoints,
)
from erpnext_moldova_banking.providers.maib.description import build_maib_transaction_description
from erpnext_moldova_banking.providers.maib.payments import (
	looks_like_transfer_identity,
	query_transfer_details,
)
from erpnext_moldova_banking.providers.maib.statement import parse_statement_xml
from erpnext_moldova_banking.utils.bank_transaction_unique_key import make_transaction_unique_key
from erpnext_moldova_banking.utils.transaction_ingest import ingest_transactions

SETTINGS_DOCTYPE = "Moldova Banking Settings"

SCHEDULE_MINUTES = {
	"Every 5 minutes": 5,
	"Every 10 minutes": 10,
	"Every 15 minutes": 15,
	"Every 30 minutes": 30,
	"Every hour": 60,
	"Every 6 hours": 6 * 60,
	"Every 12 hours": 12 * 60,
	"Daily": 24 * 60,
	"Every weekday": 24 * 60,
	"Weekly": 7 * 24 * 60,
}


def _require_system_manager():
	user = frappe.session.user
	if user == "Guest" or "System Manager" not in set(frappe.get_roles(user) or []):
		frappe.throw(_("Not permitted."), frappe.PermissionError)


def _to_yyyymmdd(value) -> str:
	return getdate(value).strftime("%Y%m%d")


def _format_stats(stats: dict[str, Any]) -> str:
	return _("Created {0}, skipped {1}, errors {2}").format(
		stats.get("created", 0),
		stats.get("skipped", 0),
		stats.get("errors", 0),
	)


def _serialize_statement_row(row: dict[str, Any]) -> dict[str, Any]:
	"""JSON-safe statement row for the manual fetch dialog."""
	out = dict(row or {})
	if out.get("date"):
		out["date"] = str(getdate(out["date"]))
	out.pop("transfer_details", None)
	return out


def _resolve_transfer_identity(row: dict[str, Any]) -> str:
	"""Pick an identity accepted by Transfer Details Query, if any."""
	for key in ("transaction_id", "document_number", "reference_number"):
		candidate = (row.get(key) or "").strip()
		if looks_like_transfer_identity(candidate):
			return candidate

	# Outward payments: DocumentNumber often equals Payment Order name.
	document_number = (row.get("document_number") or "").strip()
	if document_number and frappe.db.exists("Payment Order", document_number):
		instruction_id = (
			frappe.db.get_value("Payment Order", document_number, "maib_instruction_id") or ""
		).strip()
		if looks_like_transfer_identity(instruction_id):
			return instruction_id

	return ""


def _is_existing_bank_transaction(bank_account: str, row: dict[str, Any]) -> bool:
	company = frappe.db.get_value("Bank Account", bank_account, "company")
	unique_key = make_transaction_unique_key(
		company,
		bank_account,
		row.get("date"),
		row.get("deposit"),
		row.get("withdrawal"),
		row.get("reference_number"),
	)
	return bool(frappe.db.exists("Bank Transaction", {"unique_key": unique_key}))


def _company_party_context(bank_account: str) -> dict[str, str]:
	ba = frappe.db.get_value(
		"Bank Account",
		bank_account,
		["iban", "company", "account_name"],
		as_dict=True,
	) or {}
	company = ba.get("company") or ""
	company_idno = ""
	if company and frappe.get_meta("Company").has_field("tax_id"):
		company_idno = frappe.db.get_value("Company", company, "tax_id") or ""
	return {
		"company_iban": (ba.get("iban") or "").strip(),
		"company_name": (company or ba.get("account_name") or "").strip(),
		"company_idno": (company_idno or "").strip(),
	}


def enrich_single_row(bank_account: str, row: dict[str, Any], settings=None) -> dict[str, Any]:
	"""Try Transfer Details by TransactionId; keep statement description if it fails."""
	row = dict(row or {})
	if _is_existing_bank_transaction(bank_account, row):
		return row

	identity = _resolve_transfer_identity(row)
	if not identity:
		identity = (row.get("transaction_id") or row.get("reference_number") or "").strip()

	if not identity:
		return row

	try:
		details = query_transfer_details(identity, settings=settings, soft=True) or {}
		frappe.clear_messages()
	except Exception:
		frappe.clear_messages()
		frappe.log_error(
			frappe.get_traceback(),
			f"MAIB transfer details failed for {identity}",
		)
		return row

	if not details:
		# Keep original statement description (old format).
		return row

	ctx = _company_party_context(bank_account)
	row["transfer_details"] = details
	row["description"] = build_maib_transaction_description(
		row,
		transfer_details=details,
		company_iban=ctx["company_iban"],
		company_name=ctx["company_name"],
		company_idno=ctx["company_idno"],
	)
	return row


def enrich_new_rows_with_transfer_details(
	bank_account: str,
	rows: list[dict[str, Any]],
	settings=None,
) -> list[dict[str, Any]]:
	"""For new statement lines, load Transfer Details when possible."""
	return [enrich_single_row(bank_account, row, settings=settings) for row in (rows or [])]


def _update_sync_row_status(bank_account: str, status: str, success: bool = True):
	settings = frappe.get_single(SETTINGS_DOCTYPE)
	changed = False
	for row in settings.get("maib_sync_accounts") or []:
		if row.bank_account == bank_account:
			row.last_synced_on = now_datetime()
			row.last_sync_status = (status or "")[:140]
			changed = True
	if changed:
		settings.save(ignore_permissions=True)


@frappe.whitelist()
def get_maib_provider_defaults(environment: str | None = None) -> dict[str, str]:
	return get_maib_defaults(environment)


@frappe.whitelist()
def test_maib_connection() -> dict[str, Any]:
	_require_system_manager()
	payload = get_access_token()
	endpoints = payload.get("_endpoints") or resolve_maib_endpoints()
	return {
		"ok": True,
		"token_type": payload.get("token_type"),
		"expires_in": payload.get("expires_in"),
		"api_base_url": endpoints.get("api_base_url"),
		"environment": frappe.db.get_single_value(SETTINGS_DOCTYPE, "maib_environment"),
	}


@frappe.whitelist()
def get_sync_account_defaults(bank_account: str | None = None) -> dict[str, Any]:
	"""Return auto_submit default for a bank account from sync table, if any."""
	_require_system_manager()
	settings = frappe.get_single(SETTINGS_DOCTYPE)
	auto_submit = 0
	if bank_account:
		for row in settings.get("maib_sync_accounts") or []:
			if row.bank_account == bank_account:
				auto_submit = 1 if row.auto_submit else 0
				break
	return {"auto_submit": auto_submit}


def _validate_fetch_args(bank_account: str, from_date: str, to_date: str):
	if not bank_account:
		frappe.throw(_("Bank Account is required."))
	if not from_date or not to_date:
		frappe.throw(_("From Date and To Date are required."))
	if getdate(from_date) > getdate(to_date):
		frappe.throw(_("From Date cannot be after To Date."))


@frappe.whitelist()
def download_maib_statement(
	bank_account: str,
	from_date: str,
	to_date: str,
) -> dict[str, Any]:
	"""Download and parse statement rows for the manual fetch dialog (no ingest)."""
	_require_system_manager()
	_validate_fetch_args(bank_account, from_date, to_date)

	settings = frappe.get_single(SETTINGS_DOCTYPE)
	if not settings.maib_enabled:
		frappe.throw(_("Enable MAIB API in Moldova Banking Settings first."))

	account_id = resolve_api_account_id(bank_account)
	xml_text = fetch_statement_xml(
		account_id,
		_to_yyyymmdd(from_date),
		_to_yyyymmdd(to_date),
		settings=settings,
	)
	rows = parse_statement_xml(xml_text)
	serialized: list[dict[str, Any]] = []
	to_load = 0
	for row in rows:
		item = _serialize_statement_row(row)
		is_new = not _is_existing_bank_transaction(bank_account, row)
		item["is_new"] = is_new
		if is_new:
			to_load += 1
		serialized.append(item)

	frappe.clear_messages()
	return {
		"ok": True,
		"bank_account": bank_account,
		"from_date": str(getdate(from_date)),
		"to_date": str(getdate(to_date)),
		"fetched": len(serialized),
		"to_load": to_load,
		"rows": serialized,
	}


@frappe.whitelist()
def process_maib_statement_row(
	bank_account: str,
	row: dict | str | None = None,
	auto_submit: int | str | None = 0,
) -> dict[str, Any]:
	"""Enrich one new statement row (Transfer Details) and create Bank Transaction."""
	_require_system_manager()
	if not bank_account:
		frappe.throw(_("Bank Account is required."))
	if row is None:
		frappe.throw(_("Transaction row is required."))

	settings = frappe.get_single(SETTINGS_DOCTYPE)
	if not settings.maib_enabled:
		frappe.throw(_("Enable MAIB API in Moldova Banking Settings first."))

	if isinstance(row, str):
		row = frappe.parse_json(row)
	row = dict(row or {})
	row.pop("is_new", None)

	enriched = enrich_single_row(bank_account, row, settings=settings)
	stats = ingest_transactions(
		bank_account,
		[enriched],
		submit=bool(frappe.utils.cint(auto_submit)),
	)
	frappe.clear_messages()
	return {
		"ok": True,
		"created": stats.get("created", 0),
		"skipped": stats.get("skipped", 0),
		"errors": stats.get("errors", 0),
		"error_messages": stats.get("error_messages") or [],
	}


@frappe.whitelist()
def finalize_maib_statement_fetch(
	bank_account: str,
	fetched: int | str | None = 0,
	created: int | str | None = 0,
	skipped: int | str | None = 0,
	errors: int | str | None = 0,
) -> dict[str, Any]:
	"""Persist sync-row status after the manual stepped fetch finishes."""
	_require_system_manager()
	stats = {
		"created": frappe.utils.cint(created),
		"skipped": frappe.utils.cint(skipped),
		"errors": frappe.utils.cint(errors),
	}
	status = _format_stats(stats)
	_update_sync_row_status(bank_account, status, success=True)
	return {
		"ok": True,
		"fetched": frappe.utils.cint(fetched),
		**stats,
		"status": status,
	}


@frappe.whitelist()
def fetch_maib_statement(
	bank_account: str,
	from_date: str,
	to_date: str,
	auto_submit: int | str | None = 0,
) -> dict[str, Any]:
	"""One-shot fetch (scheduler / API). Manual UI uses stepped download/process."""
	_require_system_manager()
	_validate_fetch_args(bank_account, from_date, to_date)

	submit = frappe.utils.cint(auto_submit)
	settings = frappe.get_single(SETTINGS_DOCTYPE)
	if not settings.maib_enabled:
		frappe.throw(_("Enable MAIB API in Moldova Banking Settings first."))

	try:
		account_id = resolve_api_account_id(bank_account)
		xml_text = fetch_statement_xml(
			account_id,
			_to_yyyymmdd(from_date),
			_to_yyyymmdd(to_date),
			settings=settings,
		)
		rows = parse_statement_xml(xml_text)
		rows = enrich_new_rows_with_transfer_details(bank_account, rows, settings=settings)
		stats = ingest_transactions(bank_account, rows, submit=bool(submit))
		status = _format_stats(stats)
		_update_sync_row_status(bank_account, status, success=True)
		frappe.clear_messages()
		return {
			"ok": True,
			"bank_account": bank_account,
			"from_date": str(getdate(from_date)),
			"to_date": str(getdate(to_date)),
			"fetched": len(rows),
			**stats,
			"status": status,
		}
	except Exception as e:
		msg = str(e)
		frappe.log_error(frappe.get_traceback(), f"MAIB statement fetch failed for {bank_account}")
		_update_sync_row_status(bank_account, msg[:140], success=False)
		raise


def _is_due(row, now) -> bool:
	if row.disabled or row.schedule in (None, "", "Manual only"):
		return False

	minutes = SCHEDULE_MINUTES.get(row.schedule)
	if not minutes:
		return False

	last = get_datetime(row.last_synced_on) if row.last_synced_on else None
	if not last:
		if row.schedule == "Every weekday" and now.weekday() >= 5:
			return False
		return True

	elapsed_minutes = (now - last).total_seconds() / 60.0
	if elapsed_minutes < minutes:
		return False

	if row.schedule == "Every weekday" and now.weekday() >= 5:
		return False

	return True


def _auto_date_range(row) -> tuple[str, str]:
	to_dt = getdate(today())
	if row.last_synced_on:
		from_dt = getdate(row.last_synced_on)
	else:
		from_dt = add_days(to_dt, -7)
	if from_dt > to_dt:
		from_dt = to_dt
	return str(from_dt), str(to_dt)


def run_due_maib_statement_syncs():
	"""Scheduler entrypoint (every 5 minutes)."""
	settings = frappe.get_single(SETTINGS_DOCTYPE)
	if not settings.maib_enabled:
		return

	now = now_datetime()
	for row in list(settings.get("maib_sync_accounts") or []):
		if not _is_due(row, now):
			continue
		from_date, to_date = _auto_date_range(row)
		try:
			account_id = resolve_api_account_id(row.bank_account)
			xml_text = fetch_statement_xml(
				account_id,
				_to_yyyymmdd(from_date),
				_to_yyyymmdd(to_date),
				settings=settings,
			)
			rows = parse_statement_xml(xml_text)
			rows = enrich_new_rows_with_transfer_details(row.bank_account, rows, settings=settings)
			stats = ingest_transactions(row.bank_account, rows, submit=bool(row.auto_submit))
			_update_sync_row_status(row.bank_account, _format_stats(stats), success=True)
			frappe.db.commit()
		except Exception:
			frappe.db.rollback()
			frappe.log_error(
				frappe.get_traceback(),
				f"MAIB scheduled sync failed for {row.bank_account}",
			)
			try:
				_update_sync_row_status(row.bank_account, _("Scheduled sync failed"), success=False)
				frappe.db.commit()
			except Exception:
				frappe.db.rollback()
