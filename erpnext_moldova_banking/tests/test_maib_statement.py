# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from frappe.tests.utils import FrappeTestCase

from erpnext_moldova_banking.providers.maib.client import iban_to_maib_account_id
from erpnext_moldova_banking.providers.maib.statement import parse_statement_xml
from erpnext_moldova_banking.tests.utils import load_fixture


class TestMaibStatementParse(FrappeTestCase):
	def test_parse_swagger_style_statement(self):
		rows = parse_statement_xml(load_fixture("statement_with_txns.xml"))
		self.assertEqual(len(rows), 2)

		credit, debit = rows
		self.assertEqual(credit["deposit"], 1351.28)
		self.assertEqual(credit["withdrawal"], 0)
		self.assertEqual(credit["reference_number"], "987654321")
		self.assertEqual(credit["cp_idno"], "1506601000031")
		self.assertEqual(str(credit["date"]), "2011-02-01")
		self.assertIn("Counterparty:", credit["description"])
		self.assertIn("Document Number:", credit["description"])

		self.assertEqual(debit["deposit"], 0)
		self.assertEqual(debit["withdrawal"], 10.0)
		self.assertEqual(debit["reference_number"], "111")
		self.assertEqual(debit["cp_idno"], "1002600015382")
		self.assertIn("Counterparty:", debit["description"])

	def test_parse_live_empty_root(self):
		rows = parse_statement_xml(load_fixture("statement_empty_root.xml"))
		self.assertEqual(rows, [])

	def test_parse_pdf_empty_message(self):
		rows = parse_statement_xml(load_fixture("statement_empty_message.xml"))
		self.assertEqual(rows, [])

	def test_parse_schema_empty_transactions(self):
		rows = parse_statement_xml(load_fixture("statement_empty_transactions.xml"))
		self.assertEqual(rows, [])

	def test_parse_blank_transaction_shell_skipped(self):
		xml = """<?xml version="1.0" encoding="UTF-8"?>
		<root><Operational><Account value="1">
		<Currency>MDL</Currency>
		<Date value="20260227">
			<InitialBalance></InitialBalance>
			<FinalBalance></FinalBalance>
			<Transaction>
				<DocumentNumber></DocumentNumber>
				<ContraParty value=""><FiscalCode></FiscalCode></ContraParty>
				<PaymentDestination></PaymentDestination>
				<CreditOrDebit>C</CreditOrDebit>
				<TransactionId></TransactionId>
				<TransactionAmount></TransactionAmount>
			</Transaction>
		</Date>
		</Account></Operational></root>"""
		self.assertEqual(parse_statement_xml(xml), [])

	def test_iban_to_maib_account_id(self):
		self.assertEqual(iban_to_maib_account_id("MD24AG000000022516020091"), "22516020091")
		self.assertEqual(iban_to_maib_account_id("MD24 AG00 0000 0022 5160 20091"), "22516020091")
		self.assertEqual(iban_to_maib_account_id(""), "")
