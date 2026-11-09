# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

import frappe
from frappe.model.delete_doc import get_linked_docs
from frappe.test_runner import make_test_records
from frappe.tests.utils import FrappeTestCase
from frappe.utils import today

from erpnext.accounts.doctype.purchase_invoice.test_purchase_invoice import make_purchase_invoice

from erpnext_moldova_banking.tests.utils import (
	TEST_ITEM_CODE,
	create_maib_company_bank_account,
	create_party_bank_account,
	ensure_supplier_tax_id,
	ensure_test_item,
)

test_dependencies = ["Company", "Supplier", "Cost Center"]


class TestPurchaseInvoiceBpiLink(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		for doctype in ("Company", "Supplier", "Cost Center"):
			make_test_records(doctype, commit=True)
		ensure_test_item()

	def setUp(self):
		self.tax_id = ensure_supplier_tax_id("_Test Supplier", "1000000000005")
		self.bank_account, self.gl_account = create_maib_company_bank_account()
		self.party_bank_account = create_party_bank_account("Supplier", "_Test Supplier")

	def tearDown(self):
		frappe.db.rollback()

	def test_cancel_purchase_invoice_keeps_bank_payment_instruction(self):
		pi = make_purchase_invoice(
			supplier="_Test Supplier",
			item_code=TEST_ITEM_CODE,
			rate=100,
			qty=1,
			uom="Nos",
			warehouse="Stores - _TC",
			supplier_warehouse="Stores - _TC",
			expense_account="Cost of Goods Sold - _TC",
			cost_center=frappe.db.get_value("Company", "_Test Company", "cost_center"),
		)
		amount = pi.grand_total
		currency = frappe.db.get_value("Company", "_Test Company", "default_currency") or "INR"
		bpi = frappe.get_doc(
			{
				"doctype": "Bank Payment Instruction",
				"company": "_Test Company",
				"bank_provider": "MAIB",
				"payment_date": today(),
				"company_bank_account": self.bank_account,
				"party_type": "Supplier",
				"party": "_Test Supplier",
				"party_bank_account": self.party_bank_account,
				"amount": amount,
				"currency": currency,
				"instruction_to_bank": "Test payment",
				"invoices": [{"purchase_invoice": pi.name, "allocated_amount": amount}],
				"beneficiary_name": "_Test Supplier",
				"beneficiary_fiscal_code": self.tax_id,
				"destination_iban": "MD24TEST0000000000000001",
				"destination_bic": "AGMDMD2X",
			}
		)
		bpi.insert(ignore_permissions=True)
		bpi.submit()

		before_parent = frappe.db.get_value("Bank Payment Instruction", bpi.name, "*", as_dict=True)
		before_rows = frappe.get_all(
			"Bank Payment Instruction Invoice",
			filters={"parent": bpi.name},
			fields="*",
			order_by="idx asc",
		)

		pi.cancel()
		pi.reload()
		self.assertEqual(pi.docstatus, 2)

		after_parent = frappe.db.get_value("Bank Payment Instruction", bpi.name, "*", as_dict=True)
		after_rows = frappe.get_all(
			"Bank Payment Instruction Invoice",
			filters={"parent": bpi.name},
			fields="*",
			order_by="idx asc",
		)
		self.assertEqual(after_parent, before_parent)
		self.assertEqual(after_rows, before_rows)
		self.assertEqual(after_rows[0].purchase_invoice, pi.name)

		delete_links = get_linked_docs(pi, method="Delete")
		self.assertTrue(
			any(link["reference_doctype"] == "Bank Payment Instruction" for link in delete_links)
		)
		self.assertRaises(frappe.LinkExistsError, frappe.delete_doc, "Purchase Invoice", pi.name)
