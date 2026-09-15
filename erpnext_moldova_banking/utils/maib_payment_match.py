# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from __future__ import annotations

from typing import Any

import frappe
from frappe import _
from frappe.utils import add_days, flt, getdate

from erpnext_moldova_banking.utils.bank_transaction_automation import (
	matches_automation_rule,
	reconcile_pe_and_bt,
)
from erpnext_moldova_banking.utils.bank_payment_instruction import (
	DOCTYPE,
	_set_instruction_fields,
	get_invoice_allocations,
	is_maib_bank_account,
)

SETTINGS_DOCTYPE = "Moldova Banking Settings"
AMOUNT_TOLERANCE = 0.01
DATE_WINDOW_DAYS = 14
MIN_SCORE_AUTO = 10
MIN_SCORE_UNIQUE = 5


def is_auto_payment_entry_enabled() -> bool:
	return bool(
		frappe.db.get_single_value(SETTINGS_DOCTYPE, "maib_enabled")
		and frappe.db.get_single_value(SETTINGS_DOCTYPE, "maib_outward_payments_enabled")
		and frappe.db.get_single_value(SETTINGS_DOCTYPE, "maib_auto_payment_entry_enabled")
	)


def _used_bank_transactions() -> set[str]:
	rows = frappe.get_all(
		DOCTYPE,
		filters={"linked_bank_transaction": ["is", "set"], "docstatus": ["<", 2]},
		pluck="linked_bank_transaction",
	)
	return {r for r in rows if r}


def _supplier_tax_id(supplier: str | None) -> str:
	if not supplier:
		return ""
	return (frappe.db.get_value("Supplier", supplier, "tax_id") or "").strip()


def _score_candidate(doc, bt, supplier_tax_id: str, document_number: str) -> int:
	doc_date = getdate(doc.payment_date)
	bt_date = getdate(bt.date)
	delta = abs((bt_date - doc_date).days)
	if delta > DATE_WINDOW_DAYS:
		return -1

	score = 0
	desc = bt.description or ""

	if supplier_tax_id and supplier_tax_id in desc:
		score += 10

	if document_number:
		markers = (
			f"Document Number: {document_number}",
			f"Document Number:{document_number}",
		)
		if any(m in desc for m in markers):
			score += 10
		elif document_number in desc:
			score += 5

	if delta <= 3:
		score += 5
	elif delta <= 7:
		score += 3
	else:
		score += 1

	return score


def find_matching_bank_transaction(doc) -> str | None:
	amount = flt(doc.amount)
	if amount <= 0 or not doc.company_bank_account:
		return None

	used = _used_bank_transactions()
	if doc.get("linked_bank_transaction"):
		used.discard(doc.linked_bank_transaction)

	low = amount - AMOUNT_TOLERANCE
	high = amount + AMOUNT_TOLERANCE
	from_date = add_days(getdate(doc.payment_date), -DATE_WINDOW_DAYS)
	to_date = add_days(getdate(doc.payment_date), DATE_WINDOW_DAYS)

	# Do not use between on Currency: Frappe flt()s the [low, high] list.
	candidates = frappe.get_all(
		"Bank Transaction",
		filters=[
			["docstatus", "=", 1],
			["bank_account", "=", doc.company_bank_account],
			["date", "between", [from_date, to_date]],
			["withdrawal", ">=", low],
			["withdrawal", "<=", high],
			["unallocated_amount", ">", 0],
		],
		fields=[
			"name",
			"date",
			"withdrawal",
			"description",
			"reference_number",
			"unallocated_amount",
			"company",
			"bank_account",
		],
		order_by="date desc",
		limit=50,
	)

	supplier_tax_id = _supplier_tax_id(doc.party if doc.party_type == "Supplier" else None)
	document_number = (doc.get("document_number") or "").strip()

	scored: list[tuple[int, str]] = []
	for bt in candidates:
		if bt.name in used:
			continue
		if matches_automation_rule(bt):
			continue
		if flt(bt.unallocated_amount) + AMOUNT_TOLERANCE < amount:
			continue
		score = _score_candidate(doc, bt, supplier_tax_id, document_number)
		if score < 0:
			continue
		scored.append((score, bt.name))

	if not scored:
		return None

	scored.sort(key=lambda x: (-x[0], x[1]))
	best_score, best_name = scored[0]
	tied = [name for score, name in scored if score == best_score]
	if len(tied) > 1:
		return None
	if best_score >= MIN_SCORE_AUTO:
		return best_name
	if len(scored) == 1 and best_score >= MIN_SCORE_UNIQUE:
		return best_name
	return None


def find_matching_instruction(bt) -> str | None:
	if matches_automation_rule(bt):
		return None
	withdrawal = flt(bt.withdrawal)
	if withdrawal <= 0 or not bt.bank_account:
		return None

	used_bt = _used_bank_transactions()
	if bt.name in used_bt:
		return frappe.db.get_value(
			DOCTYPE,
			{"linked_bank_transaction": bt.name, "docstatus": ["<", 2]},
			"name",
		)

	low = withdrawal - AMOUNT_TOLERANCE
	high = withdrawal + AMOUNT_TOLERANCE
	from_date = add_days(getdate(bt.date), -DATE_WINDOW_DAYS)
	to_date = add_days(getdate(bt.date), DATE_WINDOW_DAYS)

	rows = frappe.get_all(
		DOCTYPE,
		filters=[
			["docstatus", "=", 1],
			["company_bank_account", "=", bt.bank_account],
			["status", "=", "Executed"],
			["payment_date", "between", [from_date, to_date]],
			["amount", ">=", low],
			["amount", "<=", high],
		],
		fields=["name", "payment_date", "document_number", "payment_entry", "party", "party_type", "amount"],
		limit=50,
	)
	rows = [o for o in rows if not o.payment_entry]

	scored: list[tuple[int, str]] = []
	for row in rows:
		doc = frappe.get_doc(DOCTYPE, row.name)
		supplier_tax_id = _supplier_tax_id(doc.party if doc.party_type == "Supplier" else None)
		document_number = (doc.get("document_number") or "").strip()
		score = _score_candidate(doc, bt, supplier_tax_id, document_number)
		if score < 0:
			continue
		scored.append((score, doc.name))

	if not scored:
		return None

	scored.sort(key=lambda x: (-x[0], x[1]))
	best_score, best_name = scored[0]
	tied = [name for score, name in scored if score == best_score]
	if len(tied) > 1:
		return None
	if best_score >= MIN_SCORE_AUTO or (len(scored) == 1 and best_score >= MIN_SCORE_UNIQUE):
		return best_name
	return None


def _company_bank_gl(company_bank_account: str) -> str:
	account = frappe.db.get_value("Bank Account", company_bank_account, "account")
	if not account:
		frappe.throw(_("Bank Account {0} has no linked GL Account.").format(company_bank_account))
	return account


def _create_payment_entry(doc, bt):
	allocations = get_invoice_allocations(doc)
	if not allocations:
		frappe.throw(_("Bank Payment Instruction {0} has no Purchase Invoice.").format(doc.name))

	from erpnext.accounts.doctype.payment_entry.payment_entry import get_payment_entry

	bank_gl = _company_bank_gl(doc.company_bank_account)
	first_pi, first_amt = allocations[0]
	pe = get_payment_entry(
		"Purchase Invoice",
		first_pi,
		party_amount=first_amt,
		bank_account=bank_gl,
	)
	pe.paid_from = bank_gl
	pe.posting_date = getdate(bt.date)
	pe.reference_date = getdate(bt.date)
	pe.reference_no = (bt.reference_number or doc.get("document_number") or doc.name)[:140]
	mop = frappe.db.get_single_value(SETTINGS_DOCTYPE, "automation_mode_of_payment")
	if mop:
		pe.mode_of_payment = mop
	amount = flt(doc.amount)
	if pe.payment_type == "Pay":
		pe.paid_amount = amount
		pe.received_amount = amount

	pe.set("references", [])
	for pi_name, allocated in allocations:
		pi = frappe.db.get_value(
			"Purchase Invoice",
			pi_name,
			["grand_total", "outstanding_amount", "due_date", "bill_no"],
			as_dict=True,
		) or {}
		pe.append(
			"references",
			{
				"reference_doctype": "Purchase Invoice",
				"reference_name": pi_name,
				"due_date": pi.get("due_date"),
				"total_amount": flt(pi.get("grand_total")),
				"outstanding_amount": flt(pi.get("outstanding_amount")),
				"allocated_amount": allocated,
				"bill_no": pi.get("bill_no"),
			},
		)
	pe.insert(ignore_permissions=True)
	pe.submit()
	return pe


def process_instruction_match(
	instruction: str,
	bank_transaction: str | None = None,
	*,
	force: bool = False,
) -> dict[str, Any]:
	if not force and not is_auto_payment_entry_enabled():
		return {"ok": False, "skipped": "disabled"}

	doc = frappe.get_doc(DOCTYPE, instruction)
	if doc.docstatus != 1:
		frappe.throw(_("Bank Payment Instruction {0} must be submitted.").format(instruction))

	if not is_maib_bank_account(doc.company_bank_account) and not doc.get("bank_instruction_id"):
		return {"ok": False, "skipped": "not_maib"}

	if (doc.get("status") or "") != "Executed":
		frappe.throw(
			_("Bank Payment Instruction {0} must be Executed before creating Payment Entry (status: {1}).").format(
				instruction, doc.get("status") or "Not Sent"
			)
		)

	if doc.get("payment_entry") and frappe.db.exists("Payment Entry", doc.payment_entry):
		pe_status = frappe.db.get_value("Payment Entry", doc.payment_entry, "docstatus")
		if pe_status == 1:
			bt_name = doc.get("linked_bank_transaction") or bank_transaction
			if bt_name:
				reconcile_pe_and_bt(doc.payment_entry, bt_name)
			return {
				"ok": True,
				"instruction": doc.name,
				"payment_entry": doc.payment_entry,
				"bank_transaction": bt_name,
				"already_exists": True,
			}

	bt_name = bank_transaction or doc.get("linked_bank_transaction") or find_matching_bank_transaction(doc)
	if not bt_name:
		return {"ok": False, "skipped": "no_match", "instruction": doc.name}

	bt = frappe.get_doc("Bank Transaction", bt_name)
	if bt.docstatus != 1:
		frappe.throw(_("Bank Transaction {0} must be submitted.").format(bt_name))
	if flt(bt.withdrawal) <= 0:
		frappe.throw(_("Bank Transaction {0} is not a withdrawal.").format(bt_name))
	if bt.bank_account != doc.company_bank_account:
		frappe.throw(_("Bank Transaction bank account does not match the instruction company bank account."))

	try:
		pe = _create_payment_entry(doc, bt)
		reconcile_pe_and_bt(pe, bt)
		_set_instruction_fields(
			doc.name,
			{
				"payment_entry": pe.name,
				"linked_bank_transaction": bt.name,
				"api_error": "",
			},
		)
		frappe.db.commit()
		return {
			"ok": True,
			"instruction": doc.name,
			"payment_entry": pe.name,
			"bank_transaction": bt.name,
		}
	except Exception as e:
		frappe.db.rollback()
		frappe.log_error(frappe.get_traceback(), f"MAIB instruction match failed for {doc.name}")
		_set_instruction_fields(doc.name, {"api_error": str(e)[:1000]})
		frappe.db.commit()
		raise


def try_match_after_status_update(instruction: str) -> dict[str, Any] | None:
	if not is_auto_payment_entry_enabled():
		return None
	status = frappe.db.get_value(DOCTYPE, instruction, "status")
	if status != "Executed":
		return None
	if frappe.db.get_value(DOCTYPE, instruction, "payment_entry"):
		return None
	try:
		return process_instruction_match(instruction)
	except Exception:
		frappe.log_error(frappe.get_traceback(), f"MAIB auto PE after status update failed for {instruction}")
		return None


def try_match_bank_transaction(doc, method=None):
	if getattr(frappe.flags, "moldova_bt_automation_matched", False) or matches_automation_rule(doc):
		return
	if not is_auto_payment_entry_enabled():
		return
	if flt(doc.withdrawal) <= 0:
		return
	if not is_maib_bank_account(doc.bank_account):
		return

	name = find_matching_instruction(doc)
	if not name:
		return
	try:
		process_instruction_match(name, bank_transaction=doc.name)
	except Exception:
		frappe.log_error(frappe.get_traceback(), f"MAIB auto PE from Bank Transaction {doc.name} failed")


def process_executed_instructions(limit: int = 50) -> dict[str, Any]:
	if not is_auto_payment_entry_enabled():
		return {"checked": 0, "matched": 0, "errors": 0, "skipped": "disabled"}

	rows = frappe.get_all(
		DOCTYPE,
		filters={"docstatus": 1, "status": "Executed"},
		fields=["name", "payment_entry"],
		order_by="last_sync asc, modified asc",
		limit=limit * 3,
	)
	rows = [r.name for r in rows if not r.payment_entry][:limit]

	matched = 0
	errors = 0
	for name in rows:
		try:
			result = process_instruction_match(name)
			if result.get("ok"):
				matched += 1
		except Exception:
			errors += 1
			frappe.log_error(frappe.get_traceback(), f"MAIB scheduled PE match failed for {name}")

	return {"checked": len(rows), "matched": matched, "errors": errors}


@frappe.whitelist()
def match_instruction_to_bank_transaction(name: str, bank_transaction: str | None = None) -> dict[str, Any]:
	frappe.has_permission(DOCTYPE, "write", throw=True)
	if not frappe.db.get_single_value(SETTINGS_DOCTYPE, "maib_outward_payments_enabled"):
		frappe.throw(_("MAIB Outward Payments are disabled in Moldova Banking Settings."))
	return process_instruction_match(name, bank_transaction=bank_transaction, force=True)
