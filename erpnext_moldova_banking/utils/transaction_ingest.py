# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from __future__ import annotations

from typing import Any

import frappe
from frappe.utils import flt, getdate

from erpnext_moldova_banking.utils.party import resolve_party_by_idno


def ingest_transactions(
	bank_account: str,
	rows: list[dict[str, Any]],
	submit: bool = False,
	progress_callback=None,
) -> dict[str, Any]:
	"""Create Bank Transactions from normalized rows.

	Returns stats: created, skipped, errors, created_names.
	"""
	ba = frappe.get_doc("Bank Account", bank_account)
	company = ba.company
	currency = None
	if ba.account:
		currency = frappe.db.get_value("Account", ba.account, "account_currency")

	stats = {
		"created": 0,
		"skipped": 0,
		"errors": 0,
		"created_names": [],
		"error_messages": [],
	}

	total = len(rows or [])
	for idx, row in enumerate(rows or []):
		try:
			_ingest_one(ba.name, company, currency, row, submit=submit, stats=stats)
		except frappe.ValidationError as e:
			# Duplicate unique_key raises ValidationError via frappe.throw
			msg = str(e)
			if "Duplicate bank statement line" in msg or "unique_key" in msg.lower():
				stats["skipped"] += 1
			else:
				stats["errors"] += 1
				stats["error_messages"].append(msg)
				frappe.log_error(frappe.get_traceback(), "Bank transaction ingest validation error")
		except Exception as e:
			stats["errors"] += 1
			stats["error_messages"].append(str(e))
			frappe.log_error(frappe.get_traceback(), "Bank transaction ingest failed")

		if progress_callback:
			try:
				progress_callback(idx + 1, total)
			except Exception:
				pass

	return stats


def _ingest_one(bank_account, company, currency, row, submit, stats):
	party_type, party = resolve_party_by_idno(row)

	doc = frappe.new_doc("Bank Transaction")
	doc.date = getdate(row.get("date")) if row.get("date") else None
	doc.bank_account = bank_account
	doc.company = company
	doc.deposit = flt(row.get("deposit") or 0)
	doc.withdrawal = flt(row.get("withdrawal") or 0)
	doc.description = row.get("description") or ""
	doc.reference_number = row.get("reference_number") or ""
	if currency or row.get("currency"):
		doc.currency = row.get("currency") or currency
	if party_type and party:
		doc.party_type = party_type
		doc.party = party

	# Optional statement party fields if present on BT
	meta = frappe.get_meta("Bank Transaction")
	if row.get("cp_name") and meta.has_field("bank_party_name"):
		doc.bank_party_name = row["cp_name"]
	if row.get("cp_idno") and meta.has_field("party_name"):
		pass

	doc.insert(ignore_permissions=True)
	stats["created"] += 1
	stats["created_names"].append(doc.name)

	if submit:
		doc.submit()
