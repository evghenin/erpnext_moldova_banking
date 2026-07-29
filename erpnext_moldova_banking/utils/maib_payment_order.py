# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from __future__ import annotations

from typing import Any

import frappe
from frappe import _
from frappe.utils import flt, getdate, now_datetime

from erpnext_moldova_banking.providers.maib.client import iban_to_maib_account_id
from erpnext_moldova_banking.providers.maib.payments import (
	create_ordinary_payment,
	map_maib_status,
	query_instruction_states,
)

OPEN_MAIB_STATUSES = ("Waiting For Authorisation", "In Process")
TERMINAL_MAIB_STATUSES = ("Executed", "Rejected")


def _assert_outward_payments_enabled():
	enabled = frappe.db.get_single_value("Moldova Banking Settings", "maib_outward_payments_enabled")
	if not enabled:
		frappe.throw(_("MAIB Outward Payments are disabled in Moldova Banking Settings."))


def validate_payment_order_for_maib(doc, method=None):
	"""Enforce 1 Payment Order = 1 payment reference for MAIB-ready documents."""
	if not doc.references:
		return

	# Soft rule: only when company bank is MAIB or when already using MAIB fields
	if not _is_maib_bank_account(doc.company_bank_account) and not doc.get("maib_instruction_id"):
		return

	_assert_single_reference(doc)


def _assert_single_reference(doc):
	if len(doc.references or []) != 1:
		frappe.throw(
			_("MAIB Payment Order must contain exactly one payment reference (got {0}).").format(
				len(doc.references or [])
			)
		)


def _is_maib_bank_account(bank_account: str | None) -> bool:
	if not bank_account:
		return False
	bank = frappe.db.get_value("Bank Account", bank_account, "bank")
	if not bank:
		return False
	bank_name = (bank or "").upper()
	if "MAIB" in bank_name:
		return True
	# Also accept banks whose SWIFT starts with AGMD (MAIB)
	swift = (frappe.db.get_value("Bank", bank, "swift_number") or "").upper()
	return swift.startswith("AGMD")


def _build_payment_details(doc) -> str:
	row = doc.references[0]
	parts = []
	if row.payment_request:
		pr = frappe.db.get_value(
			"Payment Request", row.payment_request, ["subject", "message"], as_dict=True
		) or {}
		for key in ("subject", "message"):
			text = (pr.get(key) or "").strip()
			if text:
				parts.append(text)
				break
	if row.reference_name:
		parts.append(f"{row.reference_doctype} {row.reference_name}")
	if row.payment_reference:
		parts.append(str(row.payment_reference))
	details = " / ".join(p for p in parts if p) or f"Payment Order {doc.name}"
	return details[:210]


def _resolve_beneficiary(doc) -> dict[str, Any]:
	row = doc.references[0]
	supplier = row.supplier
	if not supplier:
		frappe.throw(_("Payment Order {0} has no Supplier on the reference row.").format(doc.name))

	supplier_doc = frappe.get_cached_doc("Supplier", supplier)
	tax_id = (supplier_doc.tax_id or "").strip()
	if not tax_id:
		frappe.throw(_("Supplier {0} has no Tax ID (fiscal code) required by MAIB.").format(supplier))

	bank_account = row.bank_account
	if not bank_account:
		frappe.throw(_("Payment Order {0} reference has no Bank Account.").format(doc.name))

	iban, bank = frappe.db.get_value("Bank Account", bank_account, ["iban", "bank"]) or (None, None)
	iban = (iban or "").replace(" ", "").upper()
	if not iban:
		frappe.throw(_("Supplier Bank Account {0} has no IBAN.").format(bank_account))

	swift = ""
	if bank:
		swift = (frappe.db.get_value("Bank", bank, "swift_number") or "").strip().upper()
	if not swift:
		frappe.throw(_("Bank {0} has no SWIFT number.").format(bank or bank_account))

	return {
		"beneficiary_name": (supplier_doc.supplier_name or supplier)[:150],
		"beneficiary_fiscal_code": tax_id[:13],
		"destination_account_number": iban,
		"destination_bank_swift_bic": swift,
	}


def _resolve_source_account(doc) -> str:
	if not doc.company_bank_account:
		frappe.throw(_("Company Bank Account is required."))
	iban = frappe.db.get_value("Bank Account", doc.company_bank_account, "iban")
	account_id = iban_to_maib_account_id(iban)
	if not account_id:
		frappe.throw(_("Company Bank Account {0} has no IBAN.").format(doc.company_bank_account))
	return account_id


def build_ordinary_payload(doc) -> dict[str, Any]:
	_assert_single_reference(doc)
	row = doc.references[0]
	amount = flt(row.amount)
	if amount <= 0:
		frappe.throw(_("Payment amount must be greater than zero."))

	beneficiary = _resolve_beneficiary(doc)
	document_number = (doc.get("maib_document_number") or doc.name or "")[:20]
	payment_date = getdate(doc.posting_date or frappe.utils.today()).strftime("%Y%m%d")

	return {
		"document_number": document_number,
		"payment_date": payment_date,
		"amount": f"{amount:.2f}",
		"details": _build_payment_details(doc),
		"payment_type": doc.get("maib_payment_type") or "NORMAL",
		"source_account_number": _resolve_source_account(doc),
		"source_product_type": "CURRENT_ACCOUNT",
		"residency_indicator": (doc.get("maib_residency_indicator") or "R")[:1],
		**beneficiary,
	}


def _set_maib_fields(name: str, values: dict[str, Any]):
	values = dict(values)
	values["maib_last_sync"] = now_datetime()
	frappe.db.set_value("Payment Order", name, values, update_modified=False)


@frappe.whitelist()
def send_payment_order_to_maib(name: str) -> dict[str, Any]:
	"""Submit ordinary MDL transfer for a submitted Payment Order."""
	frappe.has_permission("Payment Order", "write", throw=True)
	_assert_outward_payments_enabled()
	doc = frappe.get_doc("Payment Order", name)
	if doc.docstatus != 1:
		frappe.throw(_("Submit the Payment Order before sending to MAIB."))

	status = doc.get("maib_status") or "Not Sent"
	if doc.get("maib_instruction_id") and status not in ("Not Sent", "API Error", "Rejected"):
		frappe.throw(
			_("Payment Order already sent to MAIB (status: {0}, instruction: {1}).").format(
				status, doc.maib_instruction_id
			)
		)

	_assert_single_reference(doc)
	payload = build_ordinary_payload(doc)

	# Persist document number used for the send
	if not doc.get("maib_document_number"):
		frappe.db.set_value(
			"Payment Order", doc.name, "maib_document_number", payload["document_number"], update_modified=False
		)

	try:
		result = create_ordinary_payment(payload)
	except Exception as e:
		_set_maib_fields(
			doc.name,
			{
				"maib_status": "API Error",
				"maib_api_error": str(e)[:1000],
			},
		)
		frappe.db.commit()
		raise

	_set_maib_fields(
		doc.name,
		{
			"maib_instruction_id": result["instruction_id"],
			"maib_status": "Waiting For Authorisation",
			"maib_api_error": "",
			"maib_bank_comment": "",
			"maib_document_number": payload["document_number"],
		},
	)
	frappe.db.commit()

	# Immediate status refresh when possible
	try:
		refresh_payment_order_maib_status(doc.name)
	except Exception:
		frappe.log_error(frappe.get_traceback(), "MAIB Payment Order status refresh after send")

	doc.reload()
	return {
		"name": doc.name,
		"maib_instruction_id": doc.get("maib_instruction_id"),
		"maib_status": doc.get("maib_status"),
	}


@frappe.whitelist()
def refresh_payment_order_maib_status(name: str) -> dict[str, Any]:
	"""Query MAIB state for one Payment Order."""
	frappe.has_permission("Payment Order", "write", throw=True)
	doc = frappe.get_doc("Payment Order", name)
	instruction_id = doc.get("maib_instruction_id")
	if not instruction_id:
		frappe.throw(_("Payment Order {0} has no MAIB Instruction ID.").format(name))

	try:
		states = query_instruction_states([instruction_id])
	except Exception as e:
		# 404 NotFound is common right after create / before MAIB indexes the instruction.
		http_err = getattr(frappe.local, "maib_last_http_error", None) or {}
		soft = http_err.get("http_status") == 404 or "NotFound" in str(e)
		values = {"maib_api_error": str(e)[:1000]}
		if not soft:
			values["maib_status"] = "API Error"
		_set_maib_fields(doc.name, values)
		frappe.db.commit()
		if soft:
			doc.reload()
			return {
				"name": doc.name,
				"maib_instruction_id": doc.get("maib_instruction_id"),
				"maib_status": doc.get("maib_status"),
				"soft_error": True,
			}
		raise

	if not states:
		_set_maib_fields(
			doc.name,
			{
				"maib_api_error": _("MAIB returned no status for instruction {0}.").format(instruction_id),
			},
		)
		frappe.db.commit()
		doc.reload()
		return {"name": doc.name, "maib_status": doc.get("maib_status")}

	state = states[0]
	mapped = map_maib_status(state.get("status"))
	comment_parts = [state.get("comment") or ""]
	if state.get("processing_date"):
		comment_parts.append(f"Date: {state['processing_date']}")
	if state.get("processing_time"):
		comment_parts.append(f"Time: {state['processing_time']}")
	comment = " | ".join(p for p in comment_parts if p)

	_set_maib_fields(
		doc.name,
		{
			"maib_status": mapped,
			"maib_bank_comment": comment[:1000],
			"maib_api_error": "",
		},
	)
	frappe.db.commit()

	match_result = None
	if mapped == "Executed":
		from erpnext_moldova_banking.utils.maib_payment_match import try_match_after_status_update

		match_result = try_match_after_status_update(doc.name)

	doc.reload()
	result = {
		"name": doc.name,
		"maib_instruction_id": doc.get("maib_instruction_id"),
		"maib_status": doc.get("maib_status"),
		"maib_bank_comment": doc.get("maib_bank_comment"),
		"raw_status": state.get("status"),
	}
	if match_result:
		result["payment_match"] = match_result
	return result


def poll_open_maib_payment_orders(limit: int = 50) -> dict[str, Any]:
	"""Scheduler: refresh status for Waiting / In Process Payment Orders."""
	if not frappe.db.get_single_value("Moldova Banking Settings", "maib_outward_payments_enabled"):
		return {"checked": 0, "updated": 0, "errors": 0, "skipped": "disabled"}

	rows = frappe.get_all(
		"Payment Order",
		filters={
			"docstatus": 1,
			"maib_status": ["in", list(OPEN_MAIB_STATUSES)],
			"maib_instruction_id": ["is", "set"],
		},
		fields=["name", "maib_instruction_id"],
		order_by="maib_last_sync asc, modified asc",
		limit=limit,
	)
	if not rows:
		return {"checked": 0, "updated": 0, "errors": 0}

	updated = 0
	errors = 0
	for row in rows:
		try:
			states = query_instruction_states([row.maib_instruction_id])
		except Exception as e:
			http_err = getattr(frappe.local, "maib_last_http_error", None) or {}
			soft = http_err.get("http_status") == 404 or "NotFound" in str(e)
			if soft:
				_set_maib_fields(row.name, {"maib_api_error": str(e)[:1000]})
			else:
				errors += 1
				frappe.log_error(frappe.get_traceback(), f"MAIB Payment Order poll {row.name}")
			continue

		if not states:
			_set_maib_fields(row.name, {})
			continue

		state = states[0]
		mapped = map_maib_status(state.get("status"))
		comment_parts = [state.get("comment") or ""]
		if state.get("processing_date"):
			comment_parts.append(f"Date: {state['processing_date']}")
		if state.get("processing_time"):
			comment_parts.append(f"Time: {state['processing_time']}")
		try:
			_set_maib_fields(
				row.name,
				{
					"maib_status": mapped,
					"maib_bank_comment": " | ".join(p for p in comment_parts if p)[:1000],
					"maib_api_error": "",
				},
			)
			updated += 1
			if mapped == "Executed":
				from erpnext_moldova_banking.utils.maib_payment_match import try_match_after_status_update

				try_match_after_status_update(row.name)
		except Exception:
			errors += 1
			frappe.log_error(frappe.get_traceback(), f"MAIB Payment Order poll update {row.name}")

	frappe.db.commit()
	return {"checked": len(rows), "updated": updated, "errors": errors}
