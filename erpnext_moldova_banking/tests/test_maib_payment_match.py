# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

import frappe
from frappe.test_runner import make_test_records
from frappe.tests.utils import FrappeTestCase
from frappe.utils import flt, today

from erpnext.accounts.doctype.payment_entry.payment_entry import get_payment_entry
from erpnext.accounts.doctype.purchase_invoice.test_purchase_invoice import make_purchase_invoice

from erpnext_moldova_banking.tests.utils import (
	TEST_ITEM_CODE,
	create_maib_company_bank_account,
	create_party_bank_account,
	create_submitted_bank_transaction,
	enable_maib_settings,
	ensure_supplier_tax_id,
	ensure_test_item,
)
from erpnext_moldova_banking.utils.maib_payment_match import (
	find_matching_bank_transaction,
	is_auto_payment_entry_enabled,
	process_instruction_match,
)

test_dependencies = ["Company", "Supplier", "Cost Center"]


class TestMaibPaymentMatch(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		for doctype in ("Company", "Supplier", "Cost Center"):
			make_test_records(doctype, commit=True)
		ensure_test_item()

	def setUp(self):
		self.tax_id = ensure_supplier_tax_id("_Test Supplier", "1002600015382")
		self.bank_account, self.gl_account = create_maib_company_bank_account()
		self.party_bank_account = create_party_bank_account("Supplier", "_Test Supplier")
		enable_maib_settings(outward=True, auto_pe=True, api=True)

	def tearDown(self):
		frappe.db.rollback()

	def _make_executed_instruction(self, amount: float = 250):
		pi = make_purchase_invoice(
			supplier="_Test Supplier",
			item_code=TEST_ITEM_CODE,
			rate=amount,
			qty=1,
			uom="Nos",
			warehouse="Stores - _TC",
			supplier_warehouse="Stores - _TC",
			expense_account="Cost of Goods Sold - _TC",
			cost_center=frappe.db.get_value("Company", "_Test Company", "cost_center"),
		)
		doc_number = f"DOC{frappe.generate_hash(length=6)}"
		currency = frappe.db.get_value("Company", "_Test Company", "default_currency") or "INR"
		doc = frappe.get_doc(
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
				"invoices": [
					{"purchase_invoice": pi.name, "allocated_amount": amount},
				],
				"beneficiary_name": "_Test Supplier",
				"beneficiary_fiscal_code": self.tax_id,
				"destination_iban": "MD24TEST0000000000000001",
				"destination_bic": "AGMDMD2X",
			}
		)
		doc.insert(ignore_permissions=True)
		doc.submit()
		frappe.db.set_value(
			"Bank Payment Instruction",
			doc.name,
			{
				"status": "Executed",
				"bank_instruction_id": f"INS{frappe.generate_hash(length=8)}",
				"document_number": doc_number,
			},
			update_modified=False,
		)
		doc.reload()
		return doc, pi, amount, doc_number

	def test_settings_gate(self):
		enable_maib_settings(outward=True, auto_pe=False, api=True)
		self.assertFalse(is_auto_payment_entry_enabled())
		enable_maib_settings(outward=True, auto_pe=True, api=True)
		self.assertTrue(is_auto_payment_entry_enabled())

	def test_find_matching_bank_transaction(self):
		enable_maib_settings(outward=True, auto_pe=False, api=True)
		doc, _pi, amount, doc_number = self._make_executed_instruction(amount=180)
		bt = create_submitted_bank_transaction(
			bank_account=self.bank_account,
			withdrawal=amount,
			description=(
				f"Payment\nDocument Number: {doc_number}\nCounterparty IDNO: {self.tax_id}"
			),
			reference_number=f"REF-{doc_number}",
			date=today(),
			currency="INR",
		)
		matched = find_matching_bank_transaction(doc)
		self.assertEqual(matched, bt.name)

	def test_ambiguous_match_returns_none(self):
		enable_maib_settings(outward=True, auto_pe=False, api=True)
		doc, _pi, amount, doc_number = self._make_executed_instruction(amount=190)
		desc = f"Document Number: {doc_number}\nCounterparty IDNO: {self.tax_id}"
		create_submitted_bank_transaction(
			bank_account=self.bank_account,
			withdrawal=amount,
			description=desc,
			reference_number=f"A-{doc_number}",
			currency="INR",
		)
		create_submitted_bank_transaction(
			bank_account=self.bank_account,
			withdrawal=amount,
			description=desc,
			reference_number=f"B-{doc_number}",
			currency="INR",
		)
		self.assertIsNone(find_matching_bank_transaction(doc))

	def test_process_match_creates_pe_and_reconciles(self):
		enable_maib_settings(outward=True, auto_pe=False, api=True)
		doc, pi, amount, doc_number = self._make_executed_instruction(amount=210)
		bt = create_submitted_bank_transaction(
			bank_account=self.bank_account,
			withdrawal=amount,
			description=(
				f"Payment\nDocument Number: {doc_number}\nCounterparty IDNO: {self.tax_id}"
			),
			reference_number=f"MATCH-{doc_number}",
			currency="INR",
		)

		skipped = process_instruction_match(doc.name, bank_transaction=bt.name)
		self.assertEqual(skipped.get("skipped"), "disabled")

		enable_maib_settings(outward=True, auto_pe=True, api=True)
		result = process_instruction_match(doc.name, bank_transaction=bt.name, force=True)
		self.assertTrue(result.get("ok"))
		self.assertTrue(result.get("payment_entry"))

		doc.reload()
		self.assertEqual(doc.payment_entry, result["payment_entry"])
		self.assertEqual(doc.linked_bank_transaction, bt.name)

		pe = frappe.get_doc("Payment Entry", doc.payment_entry)
		self.assertEqual(pe.docstatus, 1)

		pi.reload()
		self.assertEqual(pi.outstanding_amount, 0)

		bt.reload()
		self.assertEqual(bt.status, "Reconciled")
		self.assertEqual(bt.unallocated_amount, 0)

		again = process_instruction_match(doc.name, force=True)
		self.assertTrue(again.get("already_exists") or again.get("ok"))
		self.assertEqual(again.get("payment_entry"), pe.name)

	def test_multi_invoice_creates_pe_references(self):
		enable_maib_settings(outward=True, auto_pe=True, api=True)
		pi1 = make_purchase_invoice(
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
		pi2 = make_purchase_invoice(
			supplier="_Test Supplier",
			item_code=TEST_ITEM_CODE,
			rate=80,
			qty=1,
			uom="Nos",
			warehouse="Stores - _TC",
			supplier_warehouse="Stores - _TC",
			expense_account="Cost of Goods Sold - _TC",
			cost_center=frappe.db.get_value("Company", "_Test Company", "cost_center"),
		)
		amount = 180
		doc_number = f"DOC{frappe.generate_hash(length=6)}"
		currency = frappe.db.get_value("Company", "_Test Company", "default_currency") or "INR"
		doc = frappe.get_doc(
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
				"beneficiary_name": "_Test Supplier",
				"beneficiary_fiscal_code": self.tax_id,
				"destination_iban": "MD24TEST0000000000000001",
				"destination_bic": "AGMDMD2X",
				"invoices": [
					{"purchase_invoice": pi1.name, "allocated_amount": 100},
					{"purchase_invoice": pi2.name, "allocated_amount": 80},
				],
			}
		)
		doc.insert(ignore_permissions=True)
		doc.submit()
		frappe.db.set_value(
			"Bank Payment Instruction",
			doc.name,
			{
				"status": "Executed",
				"bank_instruction_id": f"INS{frappe.generate_hash(length=8)}",
				"document_number": doc_number,
			},
			update_modified=False,
		)
		bt = create_submitted_bank_transaction(
			bank_account=self.bank_account,
			withdrawal=amount,
			description=f"Document Number: {doc_number}\nCounterparty IDNO: {self.tax_id}",
			reference_number=f"MULTI-{doc_number}",
			currency="INR",
		)
		result = process_instruction_match(doc.name, bank_transaction=bt.name, force=True)
		self.assertTrue(result.get("ok"))
		pe = frappe.get_doc("Payment Entry", result["payment_entry"])
		self.assertEqual(len(pe.references), 2)
		refs = {row.reference_name: flt(row.allocated_amount) for row in pe.references}
		self.assertEqual(refs[pi1.name], 100)
		self.assertEqual(refs[pi2.name], 80)
		pi1.reload()
		pi2.reload()
		self.assertEqual(pi1.outstanding_amount, 0)
		self.assertEqual(pi2.outstanding_amount, 0)

	def test_no_duplicate_pe_when_already_linked(self):
		enable_maib_settings(outward=True, auto_pe=False, api=True)
		doc, pi, amount, doc_number = self._make_executed_instruction(amount=220)
		bt = create_submitted_bank_transaction(
			bank_account=self.bank_account,
			withdrawal=amount,
			description=f"Document Number: {doc_number}\nCounterparty IDNO: {self.tax_id}",
			reference_number=f"EXIST-{doc_number}",
			currency="INR",
		)
		pe = get_payment_entry("Purchase Invoice", pi.name, bank_account=self.gl_account)
		pe.reference_no = f"MANUAL-{doc_number}"
		pe.reference_date = today()
		pe.insert(ignore_permissions=True)
		pe.submit()
		frappe.db.set_value(
			"Bank Payment Instruction",
			doc.name,
			{"payment_entry": pe.name, "linked_bank_transaction": bt.name},
			update_modified=False,
		)

		result = process_instruction_match(doc.name, force=True)
		self.assertTrue(result.get("already_exists"))
		self.assertEqual(result.get("payment_entry"), pe.name)
