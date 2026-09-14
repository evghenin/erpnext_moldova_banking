# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from __future__ import annotations

from typing import Any

import frappe
from frappe import _
from frappe.utils import cint, flt, getdate, now_datetime, today

from erpnext_moldova_banking.providers.maib.payments import (
	create_ordinary_payment,
	map_maib_status,
	query_instruction_states,
)
from erpnext_moldova_banking.utils.idno_settings import get_idno_fields
from erpnext_moldova_banking.utils.payment_details import (
	build_instruction_to_bank_for_invoices,
	clean_instruction_to_bank,
	get_invoice_payment_purpose,
)

OPEN_BANK_STATUSES = ("Waiting For Authorisation", "In Process")
_MAX_DOCUMENT_NUMBER_DIGITS = 9
DOCTYPE = "Bank Payment Instruction"
ALLOWED_PARTY_TYPES = ("Company", "Customer", "Supplier")
PARTY_NAME_FIELDS = {
	"Company": "company_name",
	"Customer": "customer_name",
	"Supplier": "supplier_name",
}


def is_maib_bank_account(bank_account: str | None) -> bool:
	if not bank_account:
		return False
	bank = frappe.db.get_value("Bank Account", bank_account, "bank")
	if not bank:
		return False
	if "MAIB" in (bank or "").upper():
		return True
	swift = (frappe.db.get_value("Bank", bank, "swift_number") or "").upper()
	return swift.startswith("AGMD")


def _assert_outward_payments_enabled():
	enabled = frappe.db.get_single_value("Moldova Banking Settings", "maib_outward_payments_enabled")
	if not enabled:
		frappe.throw(_("MAIB Outward Payments are disabled in Moldova Banking Settings."))


def make_document_number(doc) -> str:
	existing = str(doc.get("document_number") or "").strip()
	if existing:
		if not existing.isdigit():
			frappe.throw(_("Document Number must contain digits only."))
		return existing

	document_name = str(doc.get("name") or "").strip()
	if not document_name:
		frappe.throw(_("Bank Payment Instruction must be saved before it can be sent to the bank."))

	next_id = _next_document_number(doc.get("company"))
	doc.document_number = next_id
	if getattr(doc, "doctype", None) == DOCTYPE and not doc.get("__islocal"):
		frappe.db.set_value(DOCTYPE, document_name, "document_number", next_id, update_modified=False)
	return next_id


def _next_document_number(company: str | None) -> str:
	company = company or ""
	rows = frappe.db.sql(
		"""
		select document_number
		from `tabBank Payment Instruction`
		where ifnull(company, '') = %s
			and ifnull(document_number, '') != ''
		for update
		""",
		(company,),
	)
	max_n = 0
	for (value,) in rows or ():
		text = str(value or "").strip()
		if text.isdigit() and len(text) <= _MAX_DOCUMENT_NUMBER_DIGITS:
			max_n = max(max_n, int(text))
	return str(max_n + 1)


def _clean_iban(value: str | None) -> str:
	return (value or "").replace(" ", "").upper()


def map_residency_status(moldova_residency_status: str | None) -> str:
	return "R" if (moldova_residency_status or "") == "Resident" else "N"


def _party_idno_field(party_type: str) -> str:
	fields = get_idno_fields()
	return fields.get(party_type.lower()) or "tax_id"


@frappe.whitelist()
def get_beneficiary_defaults(party_type: str | None = None, party: str | None = None, party_bank_account: str | None = None) -> dict:
	values = {
		"beneficiary_name": "",
		"beneficiary_fiscal_code": "",
		"destination_iban": "",
		"destination_bic": "",
		"residency_status": "N",
	}
	if party_type not in ALLOWED_PARTY_TYPES or not party:
		return values

	name_field = PARTY_NAME_FIELDS[party_type]
	idno_field = _party_idno_field(party_type)
	meta_fields = [name_field]
	if frappe.get_meta(party_type).has_field(idno_field):
		meta_fields.append(idno_field)
	if frappe.get_meta(party_type).has_field("moldova_residency_status"):
		meta_fields.append("moldova_residency_status")

	row = frappe.db.get_value(party_type, party, meta_fields, as_dict=True) or {}
	values["beneficiary_name"] = (row.get(name_field) or party or "")[:150]
	values["beneficiary_fiscal_code"] = str(row.get(idno_field) or "").strip()[:13]
	values["residency_status"] = map_residency_status(row.get("moldova_residency_status"))

	if party_bank_account:
		iban, bank = frappe.db.get_value("Bank Account", party_bank_account, ["iban", "bank"]) or (None, None)
		values["destination_iban"] = _clean_iban(iban)
		if bank:
			values["destination_bic"] = (frappe.db.get_value("Bank", bank, "swift_number") or "").strip().upper()

	return values


def _assert_allowed_party_type(party_type: str | None):
	if party_type not in ALLOWED_PARTY_TYPES:
		frappe.throw(_("Party Type must be Company, Customer or Supplier."))


def _assert_party_owns_bank_account(doc):
	if not doc.party_bank_account:
		return
	_assert_allowed_party_type(doc.party_type)
	if not doc.party:
		frappe.throw(_("Select a Party before choosing Party Bank Account."))

	account = frappe.db.get_value(
		"Bank Account",
		doc.party_bank_account,
		["name", "party_type", "party", "company", "is_company_account"],
		as_dict=True,
	)
	if not account:
		frappe.throw(_("Bank Account {0} does not exist.").format(doc.party_bank_account))

	if doc.party_type == "Company":
		owned = cint(account.is_company_account) and account.company == doc.party
	else:
		owned = account.party_type == doc.party_type and account.party == doc.party and not cint(account.is_company_account)

	if not owned:
		frappe.throw(_("Party Bank Account must belong to {0} {1}.").format(doc.party_type, doc.party))


def prepare_instruction(doc):
	_assert_allowed_party_type(doc.party_type)
	_assert_party_owns_bank_account(doc)

	defaults = get_beneficiary_defaults(doc.party_type, doc.party, doc.party_bank_account)
	doc.beneficiary_name = defaults["beneficiary_name"]
	doc.beneficiary_fiscal_code = defaults["beneficiary_fiscal_code"]
	doc.destination_iban = defaults["destination_iban"]
	doc.destination_bic = defaults["destination_bic"]
	doc.residency_status = defaults["residency_status"]

	if doc.company_bank_account:
		doc.source_iban = _clean_iban(frappe.db.get_value("Bank Account", doc.company_bank_account, "iban"))

	_sync_invoices(doc)

	invoice_names = [name for name, _amount in get_invoice_allocations(doc)]
	documents = [
		(row.get("reference_description") or "").strip()
		for row in doc.get("invoices") or []
		if (row.get("reference_description") or "").strip()
	]
	if invoice_names or documents:
		doc.instruction_to_bank = build_instruction_to_bank_for_invoices(
			invoice_names, documents=documents or None
		)

	if doc.instruction_to_bank:
		doc.instruction_to_bank = clean_instruction_to_bank(doc.instruction_to_bank)

	if not doc.currency and doc.company:
		doc.currency = frappe.db.get_value("Company", doc.company, "default_currency")

	if not doc.status:
		doc.status = "Not Sent"


def get_invoice_allocations(doc) -> list[tuple[str, float]]:
	rows = []
	for row in doc.get("invoices") or []:
		name = (row.get("purchase_invoice") or "").strip()
		amount = flt(row.get("allocated_amount"))
		if name and amount > 0:
			rows.append((name, amount))
	if not rows and doc.get("purchase_invoice") and flt(doc.get("amount")) > 0:
		rows.append((doc.purchase_invoice, flt(doc.amount)))
	return rows


def find_open_instruction_for_invoice(purchase_invoice: str, exclude: str | None = None) -> str | None:
	if not purchase_invoice:
		return None

	parents: list[str] = []
	if frappe.db.table_exists("Bank Payment Instruction Invoice"):
		parents.extend(
			frappe.get_all(
				"Bank Payment Instruction Invoice",
				filters={"purchase_invoice": purchase_invoice},
				pluck="parent",
			)
		)
	if frappe.db.has_column(DOCTYPE, "purchase_invoice"):
		legacy = frappe.get_all(
			DOCTYPE,
			filters={"purchase_invoice": purchase_invoice, "docstatus": ["<", 2]},
			pluck="name",
		)
		for name in legacy:
			if name not in parents:
				parents.append(name)

	for parent in parents:
		if exclude and parent == exclude:
			continue
		row = frappe.db.get_value(DOCTYPE, parent, ["name", "docstatus", "status"], as_dict=True)
		if not row or cint(row.docstatus) == 2:
			continue
		if (row.status or "") == "Rejected":
			continue
		return row.name
	return None


@frappe.whitelist()
def get_purchase_invoice_row(purchase_invoice: str) -> dict[str, Any]:
	frappe.has_permission(DOCTYPE, "create", throw=True)
	pi = frappe.db.get_value(
		"Purchase Invoice",
		purchase_invoice,
		["name", "bill_no", "bill_date", "outstanding_amount", "grand_total", "supplier", "company", "currency"],
		as_dict=True,
	)
	if not pi:
		frappe.throw(_("Purchase Invoice {0} does not exist.").format(purchase_invoice))
	purpose = get_invoice_payment_purpose(pi.name) or {}
	return {
		"purchase_invoice": pi.name,
		"reference_description": purpose.get("document") or "",
		"outstanding_amount": flt(pi.outstanding_amount),
		"grand_total": flt(pi.grand_total),
		"supplier": pi.supplier,
		"company": pi.company,
		"currency": pi.currency,
	}


@frappe.whitelist()
def preview_instruction_to_bank(purchase_invoices=None, documents=None) -> str:
	if isinstance(purchase_invoices, str):
		purchase_invoices = frappe.parse_json(purchase_invoices)
	if isinstance(documents, str):
		documents = frappe.parse_json(documents)
	return build_instruction_to_bank_for_invoices(
		list(purchase_invoices or []),
		documents=list(documents) if documents is not None else None,
	)


def _sync_invoices(doc):
	if doc.get("purchase_invoice") and not doc.get("invoices"):
		doc.append(
			"invoices",
			{
				"purchase_invoice": doc.purchase_invoice,
				"allocated_amount": flt(doc.amount) or None,
			},
		)

	seen: set[str] = set()
	total = 0.0
	first_invoice = ""
	exclude = None if doc.get("__islocal") else doc.get("name")

	for row in doc.get("invoices") or []:
		pi_name = (row.get("purchase_invoice") or "").strip()
		if not pi_name:
			continue
		if pi_name in seen:
			frappe.throw(_("Purchase Invoice {0} is listed more than once.").format(pi_name))
		seen.add(pi_name)

		pi = frappe.db.get_value(
			"Purchase Invoice",
			pi_name,
			["name", "docstatus", "company", "supplier", "currency", "outstanding_amount"],
			as_dict=True,
		)
		if not pi:
			frappe.throw(_("Purchase Invoice {0} does not exist.").format(pi_name))
		if not first_invoice:
			first_invoice = pi_name
			if pi.currency:
				doc.currency = pi.currency
		if cint(pi.docstatus) != 1:
			frappe.throw(_("Purchase Invoice {0} must be submitted.").format(pi_name))
		if doc.company and pi.company != doc.company:
			frappe.throw(_("Purchase Invoice {0} belongs to another company.").format(pi_name))
		if doc.party_type == "Supplier" and doc.party and pi.supplier != doc.party:
			frappe.throw(_("Purchase Invoice {0} belongs to another supplier.").format(pi_name))
		if doc.currency and pi.currency and pi.currency != doc.currency:
			frappe.throw(_("Purchase Invoice {0} currency does not match the instruction.").format(pi_name))

		row.outstanding_amount = flt(pi.outstanding_amount)
		purpose = get_invoice_payment_purpose(pi_name) or {}
		row.reference_description = clean_instruction_to_bank(
			row.get("reference_description") or purpose.get("document") or ""
		)
		if flt(row.allocated_amount) <= 0:
			row.allocated_amount = row.outstanding_amount
		if flt(row.allocated_amount) <= 0:
			frappe.throw(_("Allocated amount for {0} must be greater than zero.").format(pi_name))
		if flt(row.allocated_amount) > flt(row.outstanding_amount) + 0.01:
			frappe.throw(
				_("Allocated amount for {0} cannot exceed outstanding {1}.").format(
					pi_name, row.outstanding_amount
				)
			)

		open_instruction = find_open_instruction_for_invoice(pi_name, exclude=exclude)
		if open_instruction:
			frappe.throw(
				_("Purchase Invoice {0} is already on Bank Payment Instruction {1}.").format(
					pi_name, open_instruction
				)
			)

		total += flt(row.allocated_amount)

	if seen:
		doc.amount = total


def build_ordinary_payload(doc) -> dict[str, Any]:
	amount = flt(doc.amount)
	if amount <= 0:
		frappe.throw(_("Payment amount must be greater than zero."))
	if not doc.beneficiary_name:
		frappe.throw(_("Beneficiary Name is required."))
	if not doc.beneficiary_fiscal_code:
		frappe.throw(_("Beneficiary Fiscal Code is required."))
	source = _clean_iban(doc.source_iban)
	if not source:
		frappe.throw(_("Company Bank Account {0} has no IBAN.").format(doc.company_bank_account))
	dest = _clean_iban(doc.destination_iban)
	if not dest:
		frappe.throw(_("Party Bank Account has no IBAN."))
	if not doc.destination_bic:
		frappe.throw(_("Beneficiary BIC is required."))
	details = clean_instruction_to_bank(doc.instruction_to_bank or f"Bank Payment Instruction {doc.name}")
	payment_date = getdate(doc.payment_date or today()).strftime("%Y%m%d")
	document_number = make_document_number(doc)

	return {
		"document_number": document_number,
		"payment_date": payment_date,
		"amount": f"{amount:.2f}",
		"details": details,
		"payment_type": doc.get("payment_type") or "NORMAL",
		"source_account_number": source,
		"source_product_type": doc.get("source_product_type") or "Operational",
		"residency_indicator": (doc.get("residency_status") or "R")[:1],
		"beneficiary_name": doc.beneficiary_name[:150],
		"beneficiary_fiscal_code": doc.beneficiary_fiscal_code[:13],
		"destination_account_number": dest,
		"destination_bank_swift_bic": doc.destination_bic,
	}


def _set_instruction_fields(name: str, values: dict[str, Any]):
	values = dict(values)
	values["last_sync"] = now_datetime()
	frappe.db.set_value(DOCTYPE, name, values, update_modified=False)


def _resolve_company_bank_account(company: str) -> str:
	accounts = frappe.get_all(
		"Bank Account",
		filters={"company": company, "is_company_account": 1},
		fields=["name", "bank"],
	)
	for row in accounts:
		if is_maib_bank_account(row.name):
			return row.name
	if len(accounts) == 1:
		return accounts[0].name
	frappe.throw(_("Could not determine Company Bank Account for {0}.").format(company))


def _resolve_supplier_bank_account(supplier: str) -> str | None:
	name = frappe.db.get_value(
		"Bank Account",
		{"party_type": "Supplier", "party": supplier, "is_default": 1, "is_company_account": 0},
		"name",
	)
	if name:
		return name
	accounts = frappe.get_all(
		"Bank Account",
		filters={"party_type": "Supplier", "party": supplier, "is_company_account": 0},
		pluck="name",
	)
	if len(accounts) == 1:
		return accounts[0]
	return None


@frappe.whitelist()
def make_from_purchase_invoice(purchase_invoice: str) -> dict:
	"""Create a draft Bank Payment Instruction from a Purchase Invoice."""
	frappe.has_permission(DOCTYPE, "create", throw=True)
	pi = frappe.get_doc("Purchase Invoice", purchase_invoice)
	if pi.docstatus != 1:
		frappe.throw(_("Submit the Purchase Invoice first."))
	if flt(pi.outstanding_amount) <= 0:
		frappe.throw(_("Purchase Invoice {0} has no outstanding amount.").format(pi.name))

	existing = find_open_instruction_for_invoice(pi.name)
	if existing:
		return {"name": existing, "existing": True}

	party_ba = _resolve_supplier_bank_account(pi.supplier)
	if not party_ba:
		frappe.throw(_("Set a supplier Bank Account for {0}.").format(pi.supplier))

	doc = frappe.new_doc(DOCTYPE)
	doc.company = pi.company
	doc.bank_provider = "MAIB"
	doc.payment_date = today()
	doc.company_bank_account = _resolve_company_bank_account(pi.company)
	doc.party_type = "Supplier"
	doc.party = pi.supplier
	doc.party_bank_account = party_ba
	doc.amount = flt(pi.outstanding_amount)
	doc.currency = pi.currency or frappe.db.get_value("Company", pi.company, "default_currency")
	purpose = get_invoice_payment_purpose(pi.name) or {}
	doc.append(
		"invoices",
		{
			"purchase_invoice": pi.name,
			"reference_description": clean_instruction_to_bank(purpose.get("document") or ""),
			"outstanding_amount": flt(pi.outstanding_amount),
			"allocated_amount": flt(pi.outstanding_amount),
		},
	)
	doc.instruction_to_bank = build_instruction_to_bank_for_invoices([pi.name])
	doc.insert()
	return {"name": doc.name, "existing": False}


@frappe.whitelist()
def send_instruction_to_maib(name: str) -> dict[str, Any]:
	frappe.has_permission(DOCTYPE, "write", throw=True)
	_assert_outward_payments_enabled()
	doc = frappe.get_doc(DOCTYPE, name)
	if doc.docstatus != 1:
		frappe.throw(_("Submit the Bank Payment Instruction before sending to the bank."))
	if doc.bank_provider != "MAIB":
		frappe.throw(_("Bank provider {0} is not supported.").format(doc.bank_provider))

	status = doc.get("status") or "Not Sent"
	if doc.get("bank_instruction_id") and status not in ("Not Sent", "API Error", "Rejected"):
		frappe.throw(
			_("Instruction already sent to the bank (status: {0}, instruction: {1}).").format(
				status, doc.bank_instruction_id
			)
		)

	payload = build_ordinary_payload(doc)
	if not doc.get("document_number"):
		frappe.db.set_value(DOCTYPE, doc.name, "document_number", payload["document_number"], update_modified=False)

	try:
		result = create_ordinary_payment(payload, company=doc.company)
	except Exception as e:
		_set_instruction_fields(doc.name, {"status": "API Error", "api_error": str(e)[:1000]})
		frappe.db.commit()
		raise

	_set_instruction_fields(
		doc.name,
		{
			"bank_instruction_id": result["instruction_id"],
			"status": "Waiting For Authorisation",
			"api_error": "",
			"bank_comment": "",
			"document_number": payload["document_number"],
		},
	)
	frappe.db.commit()

	try:
		refresh_instruction_status(doc.name)
	except Exception:
		frappe.log_error(frappe.get_traceback(), "MAIB instruction status refresh after send")

	doc.reload()
	return {
		"name": doc.name,
		"bank_instruction_id": doc.get("bank_instruction_id"),
		"status": doc.get("status"),
	}


@frappe.whitelist()
def refresh_instruction_status(name: str) -> dict[str, Any]:
	frappe.has_permission(DOCTYPE, "write", throw=True)
	doc = frappe.get_doc(DOCTYPE, name)
	instruction_id = doc.get("bank_instruction_id")
	if not instruction_id:
		frappe.throw(_("Bank Payment Instruction {0} has no Bank Instruction ID.").format(name))

	try:
		states = query_instruction_states([instruction_id], company=doc.company)
	except Exception as e:
		http_err = getattr(frappe.local, "maib_last_http_error", None) or {}
		soft = http_err.get("http_status") == 404 or "NotFound" in str(e)
		if soft:
			frappe.clear_messages()
		values = {"api_error": str(e)[:1000]}
		if not soft:
			values["status"] = "API Error"
		_set_instruction_fields(doc.name, values)
		frappe.db.commit()
		if soft:
			doc.reload()
			return {
				"name": doc.name,
				"bank_instruction_id": doc.get("bank_instruction_id"),
				"status": doc.get("status"),
				"soft_error": True,
			}
		raise

	if not states:
		_set_instruction_fields(
			doc.name,
			{"api_error": _("MAIB returned no status for instruction {0}.").format(instruction_id)},
		)
		frappe.db.commit()
		doc.reload()
		return {"name": doc.name, "status": doc.get("status")}

	state = states[0]
	mapped = map_maib_status(state.get("status"))
	comment_parts = [state.get("comment") or ""]
	if state.get("processing_date"):
		comment_parts.append(f"Date: {state['processing_date']}")
	if state.get("processing_time"):
		comment_parts.append(f"Time: {state['processing_time']}")
	comment = " | ".join(p for p in comment_parts if p)

	_set_instruction_fields(
		doc.name,
		{
			"status": mapped,
			"bank_comment": comment[:1000],
			"api_error": "",
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
		"bank_instruction_id": doc.get("bank_instruction_id"),
		"status": doc.get("status"),
		"bank_comment": doc.get("bank_comment"),
		"raw_status": state.get("status"),
	}
	if match_result:
		result["payment_match"] = match_result
	return result


def poll_open_instructions(limit: int = 50) -> dict[str, Any]:
	if not frappe.db.get_single_value("Moldova Banking Settings", "maib_outward_payments_enabled"):
		return {"checked": 0, "updated": 0, "errors": 0, "skipped": "disabled"}

	rows = frappe.get_all(
		DOCTYPE,
		filters={
			"docstatus": 1,
			"bank_provider": "MAIB",
			"status": ["in", list(OPEN_BANK_STATUSES)],
			"bank_instruction_id": ["is", "set"],
		},
		fields=["name", "company", "bank_instruction_id"],
		order_by="last_sync asc, modified asc",
		limit=limit,
	)
	if not rows:
		return {"checked": 0, "updated": 0, "errors": 0}

	updated = 0
	errors = 0
	for row in rows:
		try:
			states = query_instruction_states([row.bank_instruction_id], company=row.company)
		except Exception as e:
			http_err = getattr(frappe.local, "maib_last_http_error", None) or {}
			soft = http_err.get("http_status") == 404 or "NotFound" in str(e)
			if soft:
				frappe.clear_messages()
				_set_instruction_fields(row.name, {"api_error": str(e)[:1000]})
			else:
				errors += 1
				frappe.log_error(frappe.get_traceback(), f"MAIB instruction poll {row.name}")
			continue

		if not states:
			_set_instruction_fields(row.name, {})
			continue

		state = states[0]
		mapped = map_maib_status(state.get("status"))
		comment_parts = [state.get("comment") or ""]
		if state.get("processing_date"):
			comment_parts.append(f"Date: {state['processing_date']}")
		if state.get("processing_time"):
			comment_parts.append(f"Time: {state['processing_time']}")
		try:
			_set_instruction_fields(
				row.name,
				{
					"status": mapped,
					"bank_comment": " | ".join(p for p in comment_parts if p)[:1000],
					"api_error": "",
				},
			)
			updated += 1
			if mapped == "Executed":
				from erpnext_moldova_banking.utils.maib_payment_match import try_match_after_status_update

				try_match_after_status_update(row.name)
		except Exception:
			errors += 1
			frappe.log_error(frappe.get_traceback(), f"MAIB instruction poll update {row.name}")

	frappe.db.commit()
	return {"checked": len(rows), "updated": updated, "errors": errors}
