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
	if not frappe.db.has_column("Company", "tax_id"):
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
			bt.description,
			c.tax_id AS company_idno
		FROM `tabBank Transaction` bt
		INNER JOIN `tabCompany` c ON c.name = bt.company
		WHERE bt.docstatus < 2
			AND IFNULL(bt.deposit, 0) > 0
			AND IFNULL(c.tax_id, '') != ''
			AND bt.description LIKE CONCAT(%(payer_prefix)s, c.tax_id, %(like_suffix)s)
		ORDER BY bt.bank_account, bt.date, bt.name
		""",
		{"payer_prefix": "%Payer IDNO: ", "like_suffix": "%"},
		as_dict=True,
	)
	if not rows:
		return

	statement_cache: dict[tuple[str, str], list[dict]] = {}
	failures: list[str] = []
	restored = 0

	for bt in rows:
		try:
			statement_rows = _statement_rows(bt, statement_cache)
		except Exception as exc:
			frappe.log_error(frappe.get_traceback(), f"MAIB statement reload failed for {bt.name}")
			failures.append(f"{bt.name} ({exc})")
			continue

		matches = [row for row in statement_rows if statement_row_matches(bt, row)]
		if len(matches) != 1:
			reason = "no statement line" if not matches else f"{len(matches)} statement lines"
			failures.append(f"{bt.name} ({reason}, statement rows {len(statement_rows)})")
			continue

		description = build_maib_transaction_description(matches[0])
		company_idno = _digits(bt.company_idno)
		if (
			not description
			or description == (bt.description or "")
			or (company_idno and f"Payer IDNO: {company_idno}" in description)
		):
			failures.append(f"{bt.name} (statement text still names our company as payer)")
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
		frappe.log_error(message, "Restore crossed bank transaction descriptions")
		print(message)


def _digits(value: str) -> str:
	return "".join(ch for ch in (value or "") if ch.isdigit())


def _norm_party(value: str) -> str:
	return "".join((value or "").split()).casefold()


def transaction_id_from_description(description: str) -> str:
	for line in (description or "").splitlines():
		if line.startswith("Transaction ID:"):
			return line.split(":", 1)[1].strip()
	return ""


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


def _statement_rows(bt, cache: dict[tuple[str, str, str], list[dict]]) -> list[dict]:
	posting_date = getdate(bt.date)
	# The statement date and the Bank Transaction date can differ by a day.
	from_date = add_days(posting_date, -3)
	to_date = add_days(posting_date, 3)
	key = (bt.bank_account, str(from_date), str(to_date))
	if key in cache:
		return cache[key]

	account_id = resolve_api_account_id(bt.bank_account)
	frappe.db.commit()
	xml_text = fetch_statement_xml(
		account_id,
		from_date.strftime("%Y%m%d"),
		to_date.strftime("%Y%m%d"),
		company=bt.company,
	)
	cache[key] = parse_statement_xml(xml_text)
	return cache[key]
