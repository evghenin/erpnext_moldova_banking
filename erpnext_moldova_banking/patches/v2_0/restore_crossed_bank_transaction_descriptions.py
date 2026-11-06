# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

"""Restore incoming Bank Transaction descriptions taken from another payment.

A receipt was matched to an outgoing instruction that reused the same document
number. The statement line itself is still identifiable by date, amount,
document number, counterparty, and Transaction ID. Reload that day from MAIB
and write the statement description back.
"""

from __future__ import annotations

import frappe
from frappe.utils import add_days, flt, getdate

from erpnext_moldova_banking.providers.maib.client import fetch_statement_xml, resolve_api_account_id
from erpnext_moldova_banking.providers.maib.description import build_maib_transaction_description
from erpnext_moldova_banking.providers.maib.statement import parse_statement_xml


def execute():
	if not frappe.db.table_exists("Bank Transaction") or not frappe.db.table_exists("Company"):
		return
	if not frappe.db.has_column("Company", "tax_id") or not frappe.db.has_column("Customer", "tax_id"):
		return

	rows = frappe.db.sql(
		"""
		SELECT
			bt.name,
			bt.date,
			bt.company,
			bt.bank_account,
			bt.deposit,
			bt.withdrawal,
			bt.reference_number,
			bt.bank_party_name,
			bt.party_type,
			bt.description,
			c.tax_id AS company_idno,
			cust.tax_id AS party_tax_id
		FROM `tabBank Transaction` bt
		INNER JOIN `tabCompany` c ON c.name = bt.company
		INNER JOIN `tabCustomer` cust ON cust.name = bt.party
		WHERE bt.docstatus < 2
			AND bt.party_type = 'Customer'
			AND IFNULL(bt.party, '') != ''
			AND bt.description LIKE %(payer_marker)s
		ORDER BY bt.bank_account, bt.date, bt.name
		""",
		{"payer_marker": "%Payer IDNO:%"},
		as_dict=True,
	)
	rows = [bt for bt in rows if is_crossed_customer_receipt(bt)]
	if not rows:
		return

	statement_cache: dict[tuple[str, str], list[dict]] = {}
	failures: list[str] = []
	restored = 0

	for bt in rows:
		try:
			statement_rows = _statement_rows(bt, statement_cache)
		except Exception as exc:
			frappe.log_error(f"MAIB reload {bt.name}"[:140], frappe.get_traceback())
			failures.append(f"{bt.name} ({exc})")
			continue

		matches = [row for row in statement_rows if statement_row_matches(bt, row)]
		if len(matches) != 1:
			reason = "no statement line" if not matches else f"{len(matches)} statement lines"
			failures.append(f"{bt.name} ({reason}, statement rows {len(statement_rows)})")
			continue

		description = build_maib_transaction_description(matches[0])
		if not description or description == (bt.description or ""):
			failures.append(f"{bt.name} (statement text is empty or unchanged)")
			continue

		frappe.db.set_value(
			"Bank Transaction",
			bt.name,
			"description",
			description,
			update_modified=False,
		)
		restored += 1

	if restored:
		frappe.db.commit()

	if failures:
		message = (
			f"Restored {restored} bank transaction description(s). "
			f"Could not restore {len(failures)}: {'; '.join(failures)}"
		)
		# Title is limited to 140 characters. The names belong in the message body.
		frappe.log_error("Restore crossed descriptions", message)
		print(f"Restored {restored}, could not restore {len(failures)}. See Error Log.")


def _digits(value: str) -> str:
	return "".join(ch for ch in (value or "") if ch.isdigit())


def is_crossed_customer_receipt(bt) -> bool:
	"""True when the description names our company as payer and someone else as receiver.

	A transfer where the company is both payer and receiver is left unchanged.
	The linked Customer must be a different firm from the payer written in the text.
	"""
	if (bt.get("party_type") or "") != "Customer":
		return False

	description = bt.get("description") or ""
	payer_idno = _digits(_line_value(description, "Payer IDNO"))
	receiver_idno = _digits(_line_value(description, "Receiver IDNO"))
	company_idno = _digits(bt.get("company_idno") or "")
	party_tax_id = _digits(bt.get("party_tax_id") or "")

	if not company_idno or payer_idno != company_idno:
		return False
	if not receiver_idno or receiver_idno == company_idno:
		return False
	if not party_tax_id or party_tax_id == payer_idno:
		return False
	return True


def _norm_party(value: str) -> str:
	return "".join((value or "").split()).casefold()


def _line_value(description: str, label: str) -> str:
	prefix = f"{label}:"
	for line in (description or "").splitlines():
		if line.startswith(prefix):
			return line.split(":", 1)[1].strip()
	return ""


def transaction_id_from_description(description: str) -> str:
	return _line_value(description, "Transaction ID")


def date_written_from_description(description: str):
	text = _line_value(description, "Date Written")
	if not text:
		return None
	try:
		return getdate(text)
	except Exception:
		return None


def _same_transaction_id(stored: str, statement: str) -> bool:
	stored = (stored or "").strip()
	statement = (statement or "").strip()
	if not stored or not statement:
		return False
	if stored == statement:
		return True
	stored_head, _, stored_tail = stored.partition(".")
	statement_head, _, statement_tail = statement.partition(".")
	if stored_tail and statement_tail:
		return False
	return bool(stored_head) and stored_head == statement_head and stored_head.isdigit()


def statement_row_matches(bt, row: dict) -> bool:
	"""True when a statement line is the same receipt as this Bank Transaction.

	Transaction ID is enough. Document number 1 is shared by many payments, and
	the counterparty text may differ by spaces from bank_party_name.
	"""
	if abs(flt(row.get("deposit")) - flt(bt.get("deposit"))) > 0.009:
		return False
	if flt(row.get("withdrawal")) > 0.009:
		return False

	stored_txn = transaction_id_from_description(bt.get("description") or "")
	statement_txn = (row.get("transaction_id") or "").strip()
	if _same_transaction_id(stored_txn, statement_txn):
		return True
	if stored_txn and statement_txn:
		return False

	reference = (bt.get("reference_number") or "").strip()
	document_number = (row.get("document_number") or row.get("reference_number") or "").strip()
	if not reference or reference != document_number:
		return False

	party = _norm_party(bt.get("bank_party_name") or "")
	counterparty = _norm_party(row.get("cp_name") or "")
	return bool(party and counterparty and party == counterparty)


def _query_dates(bt) -> list:
	dates = []
	for raw in (date_written_from_description(bt.get("description") or ""), bt.get("date")):
		if not raw:
			continue
		day = getdate(raw)
		if day not in dates:
			dates.append(day)
	return dates


def _statement_rows(bt, cache: dict[tuple[str, str, str], list[dict]]) -> list[dict]:
	"""Load the statement days that can contain this receipt.

	Date Written is the bank document date and can differ from Bank Transaction.date.
	A short window that misses both days comes back as an empty statement.
	"""
	found: list[dict] = []
	seen: set[tuple] = set()
	for day in _query_dates(bt):
		for row in _fetch_range(bt, day, day, cache):
			identity = (
				row.get("transaction_id") or "",
				row.get("document_number") or "",
				flt(row.get("deposit")),
				flt(row.get("withdrawal")),
			)
			if identity in seen:
				continue
			seen.add(identity)
			found.append(row)
	if found:
		return found

	center = _query_dates(bt)[0]
	return _fetch_range(bt, add_days(center, -31), add_days(center, 31), cache)


def _fetch_range(bt, from_date, to_date, cache: dict[tuple[str, str, str], list[dict]]) -> list[dict]:
	key = (bt.bank_account, str(getdate(from_date)), str(getdate(to_date)))
	if key in cache:
		return cache[key]

	account_id = resolve_api_account_id(bt.bank_account)
	frappe.db.commit()
	xml_text = fetch_statement_xml(
		account_id,
		getdate(from_date).strftime("%Y%m%d"),
		getdate(to_date).strftime("%Y%m%d"),
		company=bt.company,
	)
	cache[key] = parse_statement_xml(xml_text)
	return cache[key]
