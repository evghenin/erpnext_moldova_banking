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
from frappe.utils import flt, getdate

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
			bt.description
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
		except Exception:
			frappe.log_error(frappe.get_traceback(), f"MAIB statement reload failed for {bt.name}")
			failures.append(bt.name)
			continue

		matches = [row for row in statement_rows if statement_row_matches(bt, row)]
		if len(matches) != 1:
			failures.append(bt.name)
			continue

		description = build_maib_transaction_description(matches[0])
		if not description or description == (bt.description or ""):
			failures.append(bt.name)
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
		frappe.throw(
			f"Could not restore {len(failures)} bank transaction description(s): {', '.join(failures[:20])}"
		)


def transaction_id_from_description(description: str) -> str:
	for line in (description or "").splitlines():
		if line.startswith("Transaction ID:"):
			return line.split(":", 1)[1].strip()
	return ""


def statement_row_matches(bt, row: dict) -> bool:
	"""True when a statement line is the same receipt as this Bank Transaction."""
	if abs(flt(row.get("deposit")) - flt(bt.get("deposit"))) > 0.009:
		return False
	if abs(flt(row.get("withdrawal")) - flt(bt.get("withdrawal"))) > 0.009:
		return False

	reference = (bt.get("reference_number") or "").strip()
	document_number = (row.get("document_number") or row.get("reference_number") or "").strip()
	if reference and document_number and reference != document_number:
		return False

	party = (bt.get("bank_party_name") or "").strip()
	counterparty = (row.get("cp_name") or "").strip()
	if party and counterparty and party != counterparty:
		return False

	stored_txn = transaction_id_from_description(bt.get("description") or "")
	statement_txn = (row.get("transaction_id") or "").strip()
	if stored_txn and statement_txn and stored_txn != statement_txn:
		return False
	return True


def _statement_rows(bt, cache: dict[tuple[str, str], list[dict]]) -> list[dict]:
	posting_date = str(getdate(bt.date))
	key = (bt.bank_account, posting_date)
	if key in cache:
		return cache[key]

	account_id = resolve_api_account_id(bt.bank_account)
	yyyymmdd = getdate(bt.date).strftime("%Y%m%d")
	# Release locks before the bank call. A failed day is retried on the next migrate.
	frappe.db.commit()
	xml_text = fetch_statement_xml(account_id, yyyymmdd, yyyymmdd, company=bt.company)
	cache[key] = parse_statement_xml(xml_text)
	return cache[key]
