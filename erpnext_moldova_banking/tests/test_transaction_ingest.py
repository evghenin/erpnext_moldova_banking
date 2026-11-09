# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

import frappe
from frappe.test_runner import make_test_records
from frappe.tests.utils import FrappeTestCase
from frappe.utils import add_days, today

from erpnext_moldova_banking.tests.utils import (
	create_maib_company_bank_account,
	create_submitted_bank_transaction,
	ensure_supplier_tax_id,
)
from erpnext_moldova_banking.utils.transaction_ingest import ingest_transactions

test_dependencies = ["Company", "Cost Center"]


class TestTransactionIngest(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		for doctype in test_dependencies:
			make_test_records(doctype, commit=True)

	def setUp(self):
		self.bank_account, _ = create_maib_company_bank_account()

	def tearDown(self):
		frappe.db.rollback()

	def test_ingest_creates_and_skips_duplicates(self):
		row = {
			"date": today(),
			"deposit": 0,
			"withdrawal": 100.0,
			"description": "Test line",
			"reference_number": f"INGEST-{frappe.generate_hash(length=8)}",
			"currency": "INR",
			"cp_name": "Test Payer SRL",
			"cp_idno": ensure_supplier_tax_id(),
		}
		stats1 = ingest_transactions(self.bank_account, [row], submit=False)
		self.assertEqual(stats1["created"], 1)
		self.assertEqual(stats1["skipped"], 0)
		self.assertEqual(stats1["errors"], 0)

		stats2 = ingest_transactions(self.bank_account, [row], submit=False)
		self.assertEqual(stats2["created"], 0)
		self.assertEqual(stats2["skipped"], 1)
		self.assertEqual(stats2["errors"], 0)

		shifted = dict(row)
		shifted["date"] = add_days(today(), 1)
		stats3 = ingest_transactions(self.bank_account, [shifted], submit=False)
		self.assertEqual(stats3["created"], 0)
		self.assertEqual(stats3["skipped"], 1)
		self.assertEqual(stats3["errors"], 0)

		within_window = dict(row)
		within_window["date"] = add_days(today(), 2)
		stats4 = ingest_transactions(self.bank_account, [within_window], submit=False)
		self.assertEqual(stats4["created"], 0)
		self.assertEqual(stats4["skipped"], 1)
		self.assertEqual(stats4["errors"], 0)

		far = dict(row)
		far["date"] = add_days(today(), 8)
		stats5 = ingest_transactions(self.bank_account, [far], submit=False)
		self.assertEqual(stats5["created"], 1)
		self.assertEqual(stats5["skipped"], 0)
		self.assertEqual(stats5["errors"], 0)

	def test_transaction_id_matches_across_dates_and_backfills(self):
		reference = f"INGEST-{frappe.generate_hash(length=8)}"
		transaction_id = "213681697437387.000002"
		row = {
			"date": today(),
			"deposit": 50,
			"withdrawal": 0,
			"description": "Receipt",
			"reference_number": reference,
			"document_number": reference,
			"currency": "INR",
			"cp_name": "Payer SRL",
			"transaction_id": transaction_id,
		}
		created = ingest_transactions(self.bank_account, [row], submit=False)
		self.assertEqual(created["created"], 1)
		name = created["created_names"][0]
		self.assertEqual(frappe.db.get_value("Bank Transaction", name, "transaction_id"), transaction_id)

		later = dict(row)
		later["date"] = add_days(today(), 10)
		skipped = ingest_transactions(self.bank_account, [later], submit=False)
		self.assertEqual(skipped["created"], 0)
		self.assertEqual(skipped["skipped"], 1)
		self.assertEqual(str(frappe.db.get_value("Bank Transaction", name, "date")), str(today()))

		other_account, _ = create_maib_company_bank_account()
		other = ingest_transactions(other_account, [row], submit=False)
		self.assertEqual(other["created"], 1)

		plain = dict(row)
		plain["transaction_id"] = ""
		plain["reference_number"] = f"INGEST-{frappe.generate_hash(length=8)}"
		plain["document_number"] = plain["reference_number"]
		plain["date"] = today()
		first = ingest_transactions(self.bank_account, [plain], submit=False)
		self.assertEqual(first["created"], 1)
		plain_name = first["created_names"][0]
		self.assertFalse(frappe.db.get_value("Bank Transaction", plain_name, "transaction_id"))

		filled = dict(plain)
		filled["transaction_id"] = "214498114755446.030002"
		filled["date"] = add_days(today(), 3)
		again = ingest_transactions(self.bank_account, [filled], submit=False)
		self.assertEqual(again["skipped"], 1)
		self.assertEqual(
			frappe.db.get_value("Bank Transaction", plain_name, "transaction_id"),
			"214498114755446.030002",
		)
		self.assertEqual(str(frappe.db.get_value("Bank Transaction", plain_name, "date")), str(today()))

	def test_create_bank_transaction_helper(self):
		bt = create_submitted_bank_transaction(
			bank_account=self.bank_account,
			withdrawal=15.5,
			description="helper",
		)
		self.assertEqual(bt.docstatus, 1)
		self.assertEqual(bt.withdrawal, 15.5)
