# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

"""Test helpers for erpnext_moldova_banking (fixtures + factories)."""

from __future__ import annotations

from pathlib import Path

import frappe
from frappe.utils import getdate, today

from erpnext.accounts.doctype.bank_transaction.test_bank_transaction import (
	create_bank_account,
	create_gl_account,
)

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"

TEST_ITEM_CODE = "_Test MD Banking Item"


def load_fixture(name: str) -> str:
	return (FIXTURES_DIR / name).read_text(encoding="utf-8")


def ensure_test_item(item_code: str = TEST_ITEM_CODE) -> str:
	"""Ensure an Item whose document name equals item_code (ERPNext helpers look up by name)."""
	if frappe.db.exists("Item", item_code):
		return item_code

	by_code = frappe.db.get_value("Item", {"item_code": item_code}, "name")
	if by_code and by_code != item_code:
		frappe.rename_doc("Item", by_code, item_code, force=True)
		return item_code

	item = frappe.get_doc(
		{
			"doctype": "Item",
			"name": item_code,
			"item_code": item_code,
			"item_name": item_code,
			"item_group": "All Item Groups",
			"stock_uom": "Nos",
			"is_stock_item": 0,
		}
	)
	item.flags.name_set = True
	item.insert(ignore_permissions=True)
	if item.name != item_code:
		frappe.rename_doc("Item", item.name, item_code, force=True)
	return item_code


def ensure_maib_bank(bank_name: str | None = None) -> str:
	"""Create/update a Bank recognized as MAIB by name (avoids unique SWIFT collisions)."""
	bank_name = bank_name or f"_Test MAIB {frappe.generate_hash(length=6)}"
	if not frappe.db.exists("Bank", bank_name):
		frappe.get_doc(
			{
				"doctype": "Bank",
				"bank_name": bank_name,
			}
		).insert(ignore_permissions=True)
	return bank_name


def create_maib_company_bank_account(iban_account_id: str = "22516020091") -> tuple[str, str]:
	"""Return (bank_account_name, gl_account_name) for a MAIB company account."""
	uniq = frappe.generate_hash(length=8)
	gl_account = create_gl_account(f"_Test MAIB GL {uniq}")
	bank_name = ensure_maib_bank(f"_Test MAIB Bank {uniq}")
	bank_account = create_bank_account(
		bank_name=bank_name,
		gl_account=gl_account,
		bank_account_name=f"MAIB Current {uniq}",
	)
	iban = f"MD24AG0000000{iban_account_id}"
	# pad/trim to reasonable IBAN-like length
	frappe.db.set_value(
		"Bank Account",
		bank_account,
		{
			"iban": iban,
			"is_company_account": 1,
			"company": "_Test Company",
		},
	)
	return bank_account, gl_account


def enable_maib_settings(
	*,
	outward: bool = True,
	auto_pe: bool = True,
	api: bool = True,
):
	frappe.db.set_single_value(
		"Moldova Banking Settings",
		{
			"maib_enabled": 1 if api else 0,
			"maib_outward_payments_enabled": 1 if outward else 0,
			"maib_auto_payment_entry_enabled": 1 if auto_pe else 0,
			"maib_environment": "Test",
			"maib_client_id": "test-client",
			"maib_token_url": "https://example.test/token",
			"maib_api_base_url": "https://example.test",
			"maib_scope": "payments_gateway",
		},
	)
	# Password field via set_encrypted_password if needed by get_access_token
	try:
		from frappe.utils.password import set_encrypted_password

		set_encrypted_password(
			"Moldova Banking Settings",
			"Moldova Banking Settings",
			"test-secret",
			"maib_client_secret",
		)
	except Exception:
		pass


def ensure_supplier_tax_id(supplier: str = "_Test Supplier", tax_id: str = "1002600015382") -> str:
	if not frappe.db.exists("Supplier", supplier):
		frappe.get_doc(
			{
				"doctype": "Supplier",
				"supplier_name": supplier,
				"supplier_group": "All Supplier Groups",
				"tax_id": tax_id,
			}
		).insert(ignore_permissions=True)
	else:
		frappe.db.set_value("Supplier", supplier, "tax_id", tax_id)
	return tax_id


def create_submitted_bank_transaction(
	*,
	bank_account: str,
	withdrawal: float = 0,
	deposit: float = 0,
	description: str = "",
	reference_number: str | None = None,
	date: str | None = None,
	currency: str = "INR",
):
	doc = frappe.get_doc(
		{
			"doctype": "Bank Transaction",
			"date": getdate(date or today()),
			"bank_account": bank_account,
			"company": "_Test Company",
			"withdrawal": withdrawal,
			"deposit": deposit,
			"description": description,
			"reference_number": reference_number or frappe.generate_hash(length=10),
			"currency": currency,
		}
	)
	doc.insert(ignore_permissions=True)
	doc.submit()
	return doc
