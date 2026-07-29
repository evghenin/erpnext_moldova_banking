# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from __future__ import annotations

from typing import Any

import frappe
from frappe import _
from frappe.utils import add_days, flt, getdate

from erpnext_moldova_banking.utils.bank_transaction_automation import reconcile_pe_and_bt
from erpnext_moldova_banking.utils.maib_payment_order import _is_maib_bank_account, _set_maib_fields

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
		"Payment Order",
		filters={"maib_bank_transaction": ["is", "set"], "docstatus": ["<", 2]},
		pluck="maib_bank_transaction",
	)
	return {r for r in rows if r}


def _supplier_tax_id(supplier: str | None) -> str:
	if not supplier:
		return ""
	return (frappe.db.get_value("Supplier", supplier, "tax_id") or "").strip()


def _score_candidate(po, bt, supplier_tax_id: str, document_number: str) -> int:
	"""Return match score, or -1 if outside date window."""
	po_date = getdate(po.posting_date)
	bt_date = getdate(bt.date)
	delta = abs((bt_date - po_date).days)
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


def find_matching_bank_transaction(po) -> str | None:
	"""Find a single best matching submitted Bank Transaction for an Executed Payment Order."""
	if not po.company_bank_account or not po.references:
		return None

	row = po.references[0]
	amount = flt(row.amount)
	if amount <= 0:
		return None

	used = _used_bank_transactions()
	if po.get("maib_bank_transaction"):
		used.discard(po.maib_bank_transaction)

	low = amount - AMOUNT_TOLERANCE
	high = amount + AMOUNT_TOLERANCE
	from_date = add_days(getdate(po.posting_date), -DATE_WINDOW_DAYS)
	to_date = add_days(getdate(po.posting_date), DATE_WINDOW_DAYS)

	candidates = frappe.get_all(
		"Bank Transaction",
		filters={
			"docstatus": 1,
			"bank_account": po.company_bank_account,
			"date": ["between", [from_date, to_date]],
			"withdrawal": ["between", [low, high]],
			"unallocated_amount": [">", 0],
		},
		fields=["name", "date", "withdrawal", "description", "reference_number", "unallocated_amount"],
		order_by="date desc",
		limit=50,
	)

	supplier_tax_id = _supplier_tax_id(row.supplier)
	document_number = (po.get("maib_document_number") or "").strip()

	scored: list[tuple[int, str]] = []
	for bt in candidates:
		if bt.name in used:
			continue
		if flt(bt.unallocated_amount) + AMOUNT_TOLERANCE < amount:
			continue
		score = _score_candidate(po, bt, supplier_tax_id, document_number)
		if score < 0:
			continue
		scored.append((score, bt.name))

	if not scored:
		return None

	scored.sort(key=lambda x: (-x[0], x[1]))
	best_score, best_name = scored[0]
	tied = [name for score, name in scored if score == best_score]
	if len(tied) > 1:
		frappe.logger("maib_payment_match").info(
			f"Ambiguous BT match for Payment Order {po.name}: {tied} (score={best_score})"
		)
		return None

	if best_score >= MIN_SCORE_AUTO:
		return best_name
	if len(scored) == 1 and best_score >= MIN_SCORE_UNIQUE:
		return best_name
	return None


def find_matching_payment_order(bt) -> str | None:
	"""Find a single Executed Payment Order matching a Bank Transaction withdrawal."""
	withdrawal = flt(bt.withdrawal)
	if withdrawal <= 0 or not bt.bank_account:
		return None

	used_bt = _used_bank_transactions()
	if bt.name in used_bt:
		# already linked
		existing = frappe.db.get_value(
			"Payment Order",
			{"maib_bank_transaction": bt.name, "docstatus": ["<", 2]},
			"name",
		)
		return existing

	low = withdrawal - AMOUNT_TOLERANCE
	high = withdrawal + AMOUNT_TOLERANCE
	from_date = add_days(getdate(bt.date), -DATE_WINDOW_DAYS)
	to_date = add_days(getdate(bt.date), DATE_WINDOW_DAYS)

	orders = frappe.get_all(
		"Payment Order",
		filters={
			"docstatus": 1,
			"company_bank_account": bt.bank_account,
			"maib_status": "Executed",
			"posting_date": ["between", [from_date, to_date]],
		},
		fields=["name", "posting_date", "maib_document_number", "maib_payment_entry"],
		limit=50,
	)
	orders = [o for o in orders if not o.maib_payment_entry]

	scored: list[tuple[int, str]] = []
	for row in orders:
		po = frappe.get_doc("Payment Order", row.name)
		if not po.references or len(po.references) != 1:
			continue
		amount = flt(po.references[0].amount)
		if abs(amount - withdrawal) > AMOUNT_TOLERANCE:
			continue
		supplier_tax_id = _supplier_tax_id(po.references[0].supplier)
		document_number = (po.get("maib_document_number") or "").strip()
		score = _score_candidate(po, bt, supplier_tax_id, document_number)
		if score < 0:
			continue
		scored.append((score, po.name))

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


def _create_payment_entry(po, bt):
	"""Create (or reuse) Payment Entry for Payment Order and submit it."""
	row = po.references[0]

	if po.payment_order_type == "Payment Entry" and row.reference_name:
		pe = frappe.get_doc("Payment Entry", row.reference_name)
		if pe.docstatus == 2:
			frappe.throw(_("Linked Payment Entry {0} is cancelled.").format(pe.name))
		if pe.docstatus == 0:
			_finalize_payment_entry(pe, po, bt, row)
			pe.submit()
		return pe

	if po.payment_order_type == "Payment Request" and row.payment_request:
		pr = frappe.get_doc("Payment Request", row.payment_request)
		if pr.status == "Paid":
			existing = frappe.db.get_value(
				"Payment Entry Reference",
				{"payment_request": pr.name, "docstatus": 1},
				"parent",
			)
			if existing:
				return frappe.get_doc("Payment Entry", existing)
		pe = pr.create_payment_entry(submit=False)
	elif row.reference_doctype and row.reference_name:
		from erpnext.accounts.doctype.payment_entry.payment_entry import get_payment_entry

		bank_gl = _company_bank_gl(po.company_bank_account)
		pe = get_payment_entry(
			row.reference_doctype,
			row.reference_name,
			party_amount=flt(row.amount),
			bank_account=bank_gl,
		)
		if row.payment_request:
			for ref in pe.references or []:
				ref.payment_request = row.payment_request
	else:
		frappe.throw(
			_("Payment Order {0} has no Payment Request / invoice reference to create Payment Entry.").format(
				po.name
			)
		)

	_finalize_payment_entry(pe, po, bt, row)
	pe.insert(ignore_permissions=True)
	pe.submit()
	return pe


def _finalize_payment_entry(pe, po, bt, row):
	bank_gl = _company_bank_gl(po.company_bank_account)
	pe.paid_from = bank_gl
	pe.posting_date = getdate(bt.date)
	pe.reference_date = getdate(bt.date)
	pe.reference_no = (bt.reference_number or po.get("maib_document_number") or po.name)[:140]
	if row.mode_of_payment:
		pe.mode_of_payment = row.mode_of_payment
	else:
		mop = frappe.db.get_single_value(SETTINGS_DOCTYPE, "automation_mode_of_payment")
		if mop:
			pe.mode_of_payment = mop

	# Keep allocation equal to BT / PO amount when possible
	amount = flt(row.amount)
	if pe.payment_type == "Pay":
		pe.paid_amount = amount
		pe.received_amount = amount
	if pe.references and len(pe.references) == 1:
		pe.references[0].allocated_amount = amount


def process_payment_order_match(
	payment_order: str,
	bank_transaction: str | None = None,
	*,
	force: bool = False,
) -> dict[str, Any]:
	"""
	Match Payment Order ↔ Bank Transaction, create Payment Entry, reconcile.

	force=True bypasses the Settings checkbox (manual button / explicit call).
	"""
	if not force and not is_auto_payment_entry_enabled():
		return {"ok": False, "skipped": "disabled"}

	po = frappe.get_doc("Payment Order", payment_order)
	if po.docstatus != 1:
		frappe.throw(_("Payment Order {0} must be submitted.").format(payment_order))

	if not _is_maib_bank_account(po.company_bank_account) and not po.get("maib_instruction_id"):
		return {"ok": False, "skipped": "not_maib"}

	if (po.get("maib_status") or "") != "Executed":
		frappe.throw(
			_("Payment Order {0} must be Executed before creating Payment Entry (status: {1}).").format(
				payment_order, po.get("maib_status") or "Not Sent"
			)
		)

	if len(po.references or []) != 1:
		frappe.throw(_("MAIB Payment Order must contain exactly one payment reference."))

	if po.get("maib_payment_entry") and frappe.db.exists("Payment Entry", po.maib_payment_entry):
		pe_status = frappe.db.get_value("Payment Entry", po.maib_payment_entry, "docstatus")
		if pe_status == 1:
			bt_name = po.get("maib_bank_transaction") or bank_transaction
			if bt_name:
				reconcile_pe_and_bt(po.maib_payment_entry, bt_name)
			return {
				"ok": True,
				"payment_order": po.name,
				"payment_entry": po.maib_payment_entry,
				"bank_transaction": bt_name,
				"already_exists": True,
			}

	bt_name = bank_transaction or po.get("maib_bank_transaction") or find_matching_bank_transaction(po)
	if not bt_name:
		return {"ok": False, "skipped": "no_match", "payment_order": po.name}

	bt = frappe.get_doc("Bank Transaction", bt_name)
	if bt.docstatus != 1:
		frappe.throw(_("Bank Transaction {0} must be submitted.").format(bt_name))
	if flt(bt.withdrawal) <= 0:
		frappe.throw(_("Bank Transaction {0} is not a withdrawal.").format(bt_name))
	if bt.bank_account != po.company_bank_account:
		frappe.throw(_("Bank Transaction bank account does not match Payment Order company bank account."))

	try:
		pe = _create_payment_entry(po, bt)
		reconcile_pe_and_bt(pe, bt)
		_set_maib_fields(
			po.name,
			{
				"maib_payment_entry": pe.name,
				"maib_bank_transaction": bt.name,
				"maib_api_error": "",
			},
		)
		frappe.db.commit()
		return {
			"ok": True,
			"payment_order": po.name,
			"payment_entry": pe.name,
			"bank_transaction": bt.name,
		}
	except Exception as e:
		frappe.db.rollback()
		frappe.log_error(
			frappe.get_traceback(),
			f"MAIB Payment Order match failed for {po.name}",
		)
		_set_maib_fields(po.name, {"maib_api_error": str(e)[:1000]})
		frappe.db.commit()
		raise


def try_match_after_status_update(payment_order: str) -> dict[str, Any] | None:
	"""Called when MAIB status becomes Executed."""
	if not is_auto_payment_entry_enabled():
		return None
	status = frappe.db.get_value("Payment Order", payment_order, "maib_status")
	if status != "Executed":
		return None
	if frappe.db.get_value("Payment Order", payment_order, "maib_payment_entry"):
		return None
	try:
		return process_payment_order_match(payment_order)
	except Exception:
		frappe.log_error(
			frappe.get_traceback(),
			f"MAIB auto PE after status update failed for {payment_order}",
		)
		return None


def try_match_bank_transaction(doc, method=None):
	"""Bank Transaction on_submit: match to Executed Payment Order when enabled."""
	if not is_auto_payment_entry_enabled():
		return
	if flt(doc.withdrawal) <= 0:
		return
	if not _is_maib_bank_account(doc.bank_account):
		return

	po_name = find_matching_payment_order(doc)
	if not po_name:
		return
	try:
		process_payment_order_match(po_name, bank_transaction=doc.name)
	except Exception:
		frappe.log_error(
			frappe.get_traceback(),
			f"MAIB auto PE from Bank Transaction {doc.name} failed",
		)


def process_executed_payment_orders(limit: int = 50) -> dict[str, Any]:
	"""Scheduler: process Executed Payment Orders still missing Payment Entry."""
	if not is_auto_payment_entry_enabled():
		return {"checked": 0, "matched": 0, "errors": 0, "skipped": "disabled"}

	rows = frappe.get_all(
		"Payment Order",
		filters={
			"docstatus": 1,
			"maib_status": "Executed",
		},
		fields=["name", "maib_payment_entry"],
		order_by="maib_last_sync asc, modified asc",
		limit=limit * 3,
	)
	rows = [r.name for r in rows if not r.maib_payment_entry][:limit]

	matched = 0
	errors = 0
	for name in rows:
		try:
			result = process_payment_order_match(name)
			if result.get("ok"):
				matched += 1
		except Exception:
			errors += 1
			frappe.log_error(
				frappe.get_traceback(),
				f"MAIB scheduled PE match failed for {name}",
			)

	return {"checked": len(rows), "matched": matched, "errors": errors}


@frappe.whitelist()
def match_payment_order_to_bank_transaction(
	name: str,
	bank_transaction: str | None = None,
) -> dict[str, Any]:
	"""Manual Match & Create Payment Entry from Payment Order."""
	frappe.has_permission("Payment Order", "write", throw=True)
	if not frappe.db.get_single_value(SETTINGS_DOCTYPE, "maib_outward_payments_enabled"):
		frappe.throw(_("MAIB Outward Payments are disabled in Moldova Banking Settings."))
	return process_payment_order_match(name, bank_transaction=bank_transaction, force=True)
