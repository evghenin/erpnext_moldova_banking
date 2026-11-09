# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

"""Fill Bank Transaction.transaction_id from MAIB statements.

Existing rows were imported with the actual execution date. This patch asks
for that same date (the request omits UseActualTransactionDate) and writes
the bank operation id. It does not insert rows or change posting dates.
"""

from __future__ import annotations

import frappe
from frappe.utils import add_days, get_last_day, getdate

from erpnext_moldova_banking.providers.maib.client import fetch_statement_xml, resolve_api_account_id
from erpnext_moldova_banking.providers.maib.statement import parse_statement_xml
from erpnext_moldova_banking.utils.bank_payment_instruction import is_maib_bank_account
from erpnext_moldova_banking.utils.bank_transaction_unique_key import (
	backfill_transaction_id,
	find_existing_by_transaction_id,
	find_matching_bank_transactions,
)

PATCH_DATE_WINDOW_DAYS = 1


def execute():
	if not frappe.db.table_exists("Bank Transaction"):
		return
	if not frappe.db.has_column("Bank Transaction", "transaction_id"):
		return

	for bank_account in maib_accounts_missing_transaction_id():
		try:
			backfill_account(bank_account)
		except Exception:
			frappe.log_error(
				frappe.get_traceback(),
				f"MAIB transaction_id backfill failed for {bank_account}",
			)


def maib_accounts_missing_transaction_id() -> list[str]:
	rows = frappe.db.sql(
		"""
		SELECT DISTINCT bank_account
		FROM `tabBank Transaction`
		WHERE docstatus < 2
			AND IFNULL(bank_account, '') != ''
			AND IFNULL(transaction_id, '') = ''
		"""
	)
	return [row[0] for row in rows if row[0] and is_maib_bank_account(row[0])]


def iter_statement_chunks(from_date, to_date):
	"""Yield inclusive month slices covering from_date through to_date."""
	cursor = getdate(from_date)
	end = getdate(to_date)
	while cursor <= end:
		chunk_end = min(get_last_day(cursor), end)
		yield cursor, chunk_end
		cursor = add_days(chunk_end, 1)


def backfill_account(bank_account: str) -> dict:
	span = _missing_id_span(bank_account)
	if not span:
		return {"updated": 0, "ambiguous": 0, "unmatched": 0}

	company = frappe.db.get_value("Bank Account", bank_account, "company")
	account_id = resolve_api_account_id(bank_account)
	totals = {"updated": 0, "ambiguous": 0, "unmatched": 0}
	for chunk_start, chunk_end in iter_statement_chunks(span["from_date"], span["to_date"]):
		xml_text = fetch_statement_xml(
			account_id,
			getdate(chunk_start).strftime("%Y%m%d"),
			getdate(chunk_end).strftime("%Y%m%d"),
			company=company,
			use_actual_transaction_date=None,
		)
		stats = apply_statement_transaction_ids(bank_account, parse_statement_xml(xml_text))
		for key in totals:
			totals[key] += stats[key]
	return totals


def apply_statement_transaction_ids(bank_account: str, rows: list[dict]) -> dict:
	"""Write transaction ids for statement rows that match exactly one local row."""
	updated = 0
	ambiguous: list[str] = []
	unmatched: list[str] = []

	for row in rows or []:
		transaction_id = (row.get("transaction_id") or "").strip()
		if not transaction_id:
			continue
		if find_existing_by_transaction_id(bank_account, transaction_id):
			continue

		reference = row.get("document_number") or row.get("reference_number")
		matches = find_matching_bank_transactions(
			bank_account=bank_account,
			posting_date=row.get("date"),
			reference_number=reference,
			party_name=row.get("cp_name"),
			deposit=row.get("deposit"),
			withdrawal=row.get("withdrawal"),
			currency=row.get("currency"),
			window_days=PATCH_DATE_WINDOW_DAYS,
			only_empty_transaction_id=True,
		)
		label = f"{row.get('date') or '-'} {reference or '-'} {transaction_id}"
		if len(matches) == 1 and backfill_transaction_id(matches[0], transaction_id):
			updated += 1
		elif len(matches) > 1:
			ambiguous.append(label)
		else:
			unmatched.append(label)

	if ambiguous or unmatched:
		lines = [f"Bank Account: {bank_account}"]
		if ambiguous:
			lines.append(f"Ambiguous ({len(ambiguous)}):")
			lines.extend(ambiguous[:30])
		if unmatched:
			lines.append(f"Unmatched ({len(unmatched)}):")
			lines.extend(unmatched[:30])
		frappe.log_error(
			"\n".join(lines),
			f"MAIB transaction_id backfill skipped rows for {bank_account}",
		)

	return {"updated": updated, "ambiguous": len(ambiguous), "unmatched": len(unmatched)}


def _missing_id_span(bank_account: str) -> dict | None:
	row = frappe.db.sql(
		"""
		SELECT MIN(`date`) AS from_date, MAX(`date`) AS to_date
		FROM `tabBank Transaction`
		WHERE bank_account = %(bank_account)s
			AND docstatus < 2
			AND IFNULL(transaction_id, '') = ''
		""",
		{"bank_account": bank_account},
		as_dict=True,
	)
	if not row or not row[0].get("from_date") or not row[0].get("to_date"):
		return None
	return row[0]
