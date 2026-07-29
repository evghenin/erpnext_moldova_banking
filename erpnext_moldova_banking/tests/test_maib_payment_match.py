# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

import frappe
from frappe.test_runner import make_test_records
from frappe.tests.utils import FrappeTestCase
from frappe.utils import today

from erpnext.accounts.doctype.payment_entry.payment_entry import get_payment_entry
from erpnext.accounts.doctype.payment_request.payment_request import make_payment_request
from erpnext.accounts.doctype.purchase_invoice.test_purchase_invoice import make_purchase_invoice

from erpnext_moldova_banking.tests.utils import (
	TEST_ITEM_CODE,
	create_maib_company_bank_account,
	create_submitted_bank_transaction,
	enable_maib_settings,
	ensure_supplier_tax_id,
	ensure_test_item,
)
from erpnext_moldova_banking.utils.maib_payment_match import (
	find_matching_bank_transaction,
	is_auto_payment_entry_enabled,
	process_payment_order_match,
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
		enable_maib_settings(outward=True, auto_pe=True, api=True)

	def tearDown(self):
		frappe.db.rollback()

	def _make_executed_payment_order(self, amount: float = 250):
		pi = make_purchase_invoice(
			supplier="_Test Supplier",
			item_code=TEST_ITEM_CODE,
			rate=amount,
			qty=1,
			uom="Nos",
		)
		pr = make_payment_request(
			dt="Purchase Invoice",
			dn=pi.name,
			party_type="Supplier",
			party="_Test Supplier",
			payment_request_type="Outward",
			mute_email=1,
			submit_doc=1,
			return_doc=1,
		)
		# ensure PR uses our company bank GL as payment account when possible
		if hasattr(pr, "payment_account") and self.gl_account:
			frappe.db.set_value("Payment Request", pr.name, "payment_account", self.gl_account)

		doc_number = f"DOC{frappe.generate_hash(length=6)}"
		po = frappe.get_doc(
			{
				"doctype": "Payment Order",
				"company": "_Test Company",
				"payment_order_type": "Payment Request",
				"company_bank_account": self.bank_account,
				"posting_date": today(),
				"references": [
					{
						"reference_doctype": "Purchase Invoice",
						"reference_name": pi.name,
						"amount": amount,
						"supplier": "_Test Supplier",
						"payment_request": pr.name,
						"mode_of_payment": "Cash",
						"bank_account": self.bank_account,
						"account": self.gl_account,
					}
				],
			}
		)
		po.insert(ignore_permissions=True)
		po.submit()
		frappe.db.set_value(
			"Payment Order",
			po.name,
			{
				"maib_status": "Executed",
				"maib_instruction_id": f"INS{frappe.generate_hash(length=8)}",
				"maib_document_number": doc_number,
			},
			update_modified=False,
		)
		po.reload()
		return po, pi, pr, amount, doc_number

	def test_settings_gate(self):
		enable_maib_settings(outward=True, auto_pe=False, api=True)
		self.assertFalse(is_auto_payment_entry_enabled())
		enable_maib_settings(outward=True, auto_pe=True, api=True)
		self.assertTrue(is_auto_payment_entry_enabled())

	def test_find_matching_bank_transaction(self):
		enable_maib_settings(outward=True, auto_pe=False, api=True)
		po, _pi, _pr, amount, doc_number = self._make_executed_payment_order(amount=180)
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
		matched = find_matching_bank_transaction(po)
		self.assertEqual(matched, bt.name)

	def test_ambiguous_match_returns_none(self):
		enable_maib_settings(outward=True, auto_pe=False, api=True)
		po, _pi, _pr, amount, doc_number = self._make_executed_payment_order(amount=190)
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
		self.assertIsNone(find_matching_bank_transaction(po))

	def test_process_match_creates_pe_and_reconciles(self):
		# Create BT without auto-hook matching
		enable_maib_settings(outward=True, auto_pe=False, api=True)
		po, pi, pr, amount, doc_number = self._make_executed_payment_order(amount=210)
		bt = create_submitted_bank_transaction(
			bank_account=self.bank_account,
			withdrawal=amount,
			description=(
				f"Payment\nDocument Number: {doc_number}\nCounterparty IDNO: {self.tax_id}"
			),
			reference_number=f"MATCH-{doc_number}",
			currency="INR",
		)

		skipped = process_payment_order_match(po.name, bank_transaction=bt.name)
		self.assertEqual(skipped.get("skipped"), "disabled")

		enable_maib_settings(outward=True, auto_pe=True, api=True)
		result = process_payment_order_match(po.name, bank_transaction=bt.name, force=True)
		self.assertTrue(result.get("ok"))
		self.assertTrue(result.get("payment_entry"))

		po.reload()
		self.assertEqual(po.maib_payment_entry, result["payment_entry"])
		self.assertEqual(po.maib_bank_transaction, bt.name)

		pe = frappe.get_doc("Payment Entry", po.maib_payment_entry)
		self.assertEqual(pe.docstatus, 1)

		pr.reload()
		self.assertEqual(pr.status, "Paid")

		pi.reload()
		self.assertEqual(pi.outstanding_amount, 0)

		bt.reload()
		self.assertEqual(bt.status, "Reconciled")
		self.assertEqual(bt.unallocated_amount, 0)

		again = process_payment_order_match(po.name, force=True)
		self.assertTrue(again.get("already_exists") or again.get("ok"))
		self.assertEqual(again.get("payment_entry"), pe.name)

	def test_no_duplicate_pe_when_already_linked(self):
		"""If PE already exists for PO, process must not create another."""
		enable_maib_settings(outward=True, auto_pe=False, api=True)
		po, pi, _pr, amount, doc_number = self._make_executed_payment_order(amount=220)
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
			"Payment Order",
			po.name,
			{"maib_payment_entry": pe.name, "maib_bank_transaction": bt.name},
			update_modified=False,
		)

		result = process_payment_order_match(po.name, force=True)
		self.assertTrue(result.get("already_exists"))
		self.assertEqual(result.get("payment_entry"), pe.name)
