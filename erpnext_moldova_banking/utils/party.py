# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from __future__ import annotations

import frappe


def resolve_party_by_idno(tx: dict) -> tuple[str, str]:
	"""Resolve party_type and party from counterparty IDNO.

	Incoming (deposit) → Customer; outgoing (withdrawal) → Supplier.
	"""
	cp_idno = (tx.get("cp_idno") or "").strip()
	if not cp_idno:
		return "", ""

	try:
		settings = frappe.get_single("Moldova Banking Settings")
	except Exception:
		return "", ""

	customer_idno_field = (getattr(settings, "customer_idno_field", None) or "").strip()
	supplier_idno_field = (getattr(settings, "supplier_idno_field", None) or "").strip()

	if tx.get("deposit") and not tx.get("withdrawal") and customer_idno_field:
		customer = frappe.get_all(
			"Customer",
			filters={customer_idno_field: cp_idno},
			pluck="name",
			limit=1,
		)
		if customer:
			return "Customer", customer[0]

	if tx.get("withdrawal") and not tx.get("deposit") and supplier_idno_field:
		supplier = frappe.get_all(
			"Supplier",
			filters={supplier_idno_field: cp_idno},
			pluck="name",
			limit=1,
		)
		if supplier:
			return "Supplier", supplier[0]

	return "", ""
