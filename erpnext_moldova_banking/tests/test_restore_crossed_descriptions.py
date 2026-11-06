# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from frappe.tests.utils import FrappeTestCase

from erpnext_moldova_banking.patches.v2_0.restore_crossed_bank_transaction_descriptions import (
	date_written_from_description,
	statement_row_matches,
	transaction_id_from_description,
)
from erpnext_moldova_banking.providers.maib.description import build_maib_transaction_description


class TestRestoreCrossedDescriptions(FrappeTestCase):
	def test_transaction_id_is_read_from_description(self):
		description = "Wrong payment\n\nTransaction ID: 214498114755446.030002\nPayer IDNO: 1000000000001"
		self.assertEqual(transaction_id_from_description(description), "214498114755446.030002")
		self.assertEqual(str(date_written_from_description("Date Written: 18.09.2026")), "2026-09-18")

	def test_transaction_id_and_amount_match_without_the_same_party_text(self):
		bt = {
			"deposit": 2800,
			"withdrawal": 0,
			"reference_number": "1",
			"bank_party_name": "Payer Company SRL",
			"description": "Transaction ID: 111.030002\nPayer IDNO: 1000000000001",
		}
		same = {
			"deposit": 2800,
			"withdrawal": 0,
			"document_number": "1",
			"cp_name": "Payer   Company SRL",
			"transaction_id": "111",
			"payment_destination": "Payment for goods",
			"credit_or_debit": "C",
			"cp_idno": "1000000000003",
			"date": "2026-09-21",
		}
		other_txn = {**same, "transaction_id": "222.030002", "cp_name": "Payer Company SRL"}

		self.assertTrue(statement_row_matches(bt, same))
		self.assertFalse(statement_row_matches(bt, other_txn))

	def test_restored_description_uses_statement_payer(self):
		row = {
			"deposit": 2800,
			"withdrawal": 0,
			"amount": 2800,
			"document_number": "1",
			"cp_name": "Payer Company SRL",
			"cp_idno": "1000000000003",
			"transaction_id": "111.030002",
			"payment_destination": "Payment for goods",
			"credit_or_debit": "C",
			"date": "2026-09-21",
			"currency": "MDL",
		}
		description = build_maib_transaction_description(row)
		self.assertIn("Payment for goods", description)
		self.assertIn("Payer IDNO: 1000000000003", description)
		self.assertNotIn("Payer IDNO: 1000000000001", description)
