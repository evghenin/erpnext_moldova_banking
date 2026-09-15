# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from __future__ import annotations

import json
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
from erpnext_moldova_banking.utils.bank_transaction_unique_key import find_existing_bank_transaction
from erpnext_moldova_banking.utils.transaction_ingest import ingest_transactions

SETTINGS_DOCTYPE = "Moldova Banking Settings"


def _normalize_company_selection(value: Any) -> list[str]:
	"""Convert JSON-array / comma-separated / list values into plain company names."""
	if value is None:
		return []
	if isinstance(value, str):
		items = [value]
	elif isinstance(value, (list, tuple, set)):
		items = list(value)
	else:
		items = [value]

	normalized: list[str] = []
	for item in items:
		if item is None:
			continue
		text = str(item).strip()
		if not text:
			continue
		if text.startswith("[") and text.endswith("]"):
			try:
				parsed = json.loads(text)
				normalized.extend(_normalize_company_selection(parsed))
				continue
			except Exception:
				pass
		if "," in text:
			for part in text.split(","):
				clean = part.strip()
				if clean:
					normalized.append(clean)
			continue
		normalized.append(text)
	return normalized


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

	# Outward payments: DocumentNumber is Bank Payment Instruction.document_number.
	document_number = (row.get("document_number") or "").strip()
	if document_number:
		instruction_id = (
			frappe.db.get_value(
				"Bank Payment Instruction",
				{"document_number": document_number, "docstatus": ["<", 2]},
				"bank_instruction_id",
			)
			or ""
		).strip()
		if looks_like_transfer_identity(instruction_id):
			return instruction_id

	return ""


def _is_existing_bank_transaction(bank_account: str, row: dict[str, Any]) -> bool:
	return bool(
		find_existing_bank_transaction(
			bank_account=bank_account,
			posting_date=row.get("date"),
			reference_number=row.get("document_number") or row.get("reference_number"),
			party_name=row.get("cp_name"),
			deposit=row.get("deposit"),
			withdrawal=row.get("withdrawal"),
			currency=row.get("currency"),
		)
	)


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

	company = frappe.db.get_value("Bank Account", bank_account, "company") or ""
	try:
		details = query_transfer_details(identity, settings=settings, company=company, soft=True) or {}
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


def _commit_idle_transaction():
	"""Drop a read-only transaction so later HTTP calls do not hold InnoDB locks."""
	frappe.db.commit()


def _update_sync_row_status(bank_account: str, status: str, success: bool = True):
	"""Patch the sync child row only — do not save Moldova Banking Settings.

	Saving the Single re-runs Password field handling against `__Auth` and
	contends with desk saves of MAIB company client secrets.
	"""
	name = frappe.db.get_value(
		"MAIB Statement Sync Account",
		{
			"parent": SETTINGS_DOCTYPE,
			"parenttype": SETTINGS_DOCTYPE,
			"parentfield": "maib_sync_accounts",
			"bank_account": bank_account,
		},
		"name",
	)
	if not name:
		return
	frappe.db.set_value(
		"MAIB Statement Sync Account",
		name,
		{
			"last_synced_on": now_datetime(),
			"last_sync_status": (status or "")[:140],
		},
		update_modified=False,
	)


@frappe.whitelist()
def get_maib_provider_defaults(environment: str | None = None) -> dict[str, str]:
	return get_maib_defaults(environment)


@frappe.whitelist()
def test_maib_connection(company: str | None = None, companies: list[str] | str | None = None) -> dict[str, Any]:
	_require_system_manager()

	# Handle companies parameter: could be a list, JSON string, or comma-separated string
	selection = companies if companies is not None else ([company] if company else [])
	selection = _normalize_company_selection(selection)

	settings_doc = frappe.get_single(SETTINGS_DOCTYPE)
	if not getattr(settings_doc, "maib_enabled", 0) in (True, 1, "1", "true"):
		frappe.throw(_("MAIB API is disabled in Moldova Banking Settings."))

	rows = settings_doc.get("maib_company_settings") or []
	if not selection:
		selection = [row.company for row in rows if row.company]
	if not selection:
		frappe.throw(_("No companies configured for MAIB credentials."))

	_commit_idle_transaction()

	results: list[dict[str, Any]] = []
	for company_name in selection:
		try:
			payload = get_access_token(company=company_name)
			endpoints = resolve_maib_endpoints()
			environment = settings_doc.maib_environment or "Test"
			results.append({
				"company": company_name,
				"ok": True,
				"token_type": payload.get("token_type"),
				"expires_in": payload.get("expires_in"),
				"api_base_url": endpoints.get("api_base_url"),
				"environment": environment,
			})
		except frappe.ValidationError as e:
			results.append({
				"company": company_name,
				"ok": False,
				"error": str(e),
			})

	if not any(r.get("ok") for r in results):
		frappe.throw(_("No active MAIB configuration found for the selected companies."))

	ocurrent = next((r for r in results if r.get("ok")), {})
	return {
		"ok": True,
		"companies": results,
		"company": ocurrent.get("company"),
		"token_type": ocurrent.get("token_type"),
		"expires_in": ocurrent.get("expires_in"),
		"api_base_url": ocurrent.get("api_base_url"),
		"environment": ocurrent.get("environment"),
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
	company: str | None = None,
	companies: list[str] | str | None = None,
) -> dict[str, Any]:
	"""Download and parse statement rows for the manual fetch dialog (no ingest)."""
	_require_system_manager()
	_validate_fetch_args(bank_account, from_date, to_date)

	# Handle companies parameter: could be a list, JSON string, or comma-separated string
	selection = companies if companies is not None else ([company] if company else [])
	selection = _normalize_company_selection(selection)

	settings_doc = frappe.get_single(SETTINGS_DOCTYPE)
	if not getattr(settings_doc, "maib_enabled", 0) in (True, 1, "1", "true"):
		frappe.throw(_("MAIB API is disabled in Moldova Banking Settings."))

	rows = settings_doc.get("maib_company_settings") or []
	if not selection:
		selection = [row.company for row in rows if row.company]
	if not selection:
		frappe.throw(_("No companies configured for MAIB credentials."))

	company_name = selection[0]
	company_row = next((r for r in rows if (r.company or "") == company_name), None)
	if company_row is None:
		frappe.throw(_("No MAIB credentials configured for company {0}.").format(company_name))

	account_id = resolve_api_account_id(bank_account)
	_commit_idle_transaction()
	xml_text = fetch_statement_xml(
		account_id,
		_to_yyyymmdd(from_date),
		_to_yyyymmdd(to_date),
		company=company_name,
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

	_commit_idle_transaction()
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

	company = frappe.db.get_value("Bank Account", bank_account, "company")
	try:
		account_id = resolve_api_account_id(bank_account)
		_commit_idle_transaction()
		xml_text = fetch_statement_xml(
			account_id,
			_to_yyyymmdd(from_date),
			_to_yyyymmdd(to_date),
			company=company,
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


def _is_blank_hour(value) -> bool:
	return value is None or value == ""


def _parse_hour(value, label: str) -> int:
	try:
		hour = int(value)
	except (TypeError, ValueError):
		frappe.throw(_("{0} must be an hour between 0 and 23.").format(label))
	if hour < 0 or hour > 23:
		frappe.throw(_("{0} must be an hour between 0 and 23.").format(label))
	return hour


def validate_sync_hours(hours_from, hours_to) -> tuple[int, int] | None:
	"""Require both hours empty or both set (0–23). Returns (from, to) or None."""
	from_blank = _is_blank_hour(hours_from)
	to_blank = _is_blank_hour(hours_to)
	if from_blank and to_blank:
		return None
	if from_blank or to_blank:
		frappe.throw(_("Fill both Hours From and Hours To, or leave both empty."))
	return _parse_hour(hours_from, _("Hours From")), _parse_hour(hours_to, _("Hours To"))


def _within_sync_hours(row, now) -> bool:
	window = validate_sync_hours(getattr(row, "hours_from", None), getattr(row, "hours_to", None))
	if window is None:
		return True
	hours_from, hours_to = window
	hour = now.hour
	if hours_from <= hours_to:
		return hours_from <= hour <= hours_to
	# Overnight window, e.g. 22–6.
	return hour >= hours_from or hour <= hours_to


def _is_due(row, now) -> bool:
	if row.disabled or row.schedule in (None, "", "Manual only"):
		return False

	if not _within_sync_hours(row, now):
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
	due_rows = [row for row in list(settings.get("maib_sync_accounts") or []) if _is_due(row, now)]
	_commit_idle_transaction()

	for row in due_rows:
		from_date, to_date = _auto_date_range(row)
		try:
			company = frappe.db.get_value("Bank Account", row.bank_account, "company")
			account_id = resolve_api_account_id(row.bank_account)
			_commit_idle_transaction()
			xml_text = fetch_statement_xml(
				account_id,
				_to_yyyymmdd(from_date),
				_to_yyyymmdd(to_date),
				company=company,
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
