# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from unittest.mock import MagicMock, patch

import frappe
from frappe.tests.utils import FrappeTestCase

from erpnext_moldova_banking.providers.maib.payments import (
	append_transfer_details_to_description,
	build_ordinary_payment_xml,
	format_transfer_details_description,
	looks_like_transfer_identity,
	map_maib_status,
	parse_transfer_details_xml,
	query_instruction_states,
	query_transfer_details,
)
from erpnext_moldova_banking.utils.maib_sync import enrich_new_rows_with_transfer_details
from erpnext_moldova_banking.tests.utils import enable_maib_settings
from erpnext_moldova_banking.utils.maib_payment_order import send_payment_order_to_maib


class TestMaibPaymentsClient(FrappeTestCase):
	def tearDown(self):
		frappe.db.rollback()

	def test_status_map(self):
		self.assertEqual(map_maib_status("RequiresAction"), "Waiting For Authorisation")
		self.assertEqual(map_maib_status("Succeeded"), "Executed")
		self.assertEqual(map_maib_status("Rejected"), "Rejected")
		self.assertEqual(map_maib_status(""), "API Error")
		self.assertEqual(map_maib_status("SomethingNew"), "In Process")

	def test_build_ordinary_payment_xml(self):
		xml = build_ordinary_payment_xml(
			{
				"document_number": "PMO-1",
				"payment_date": "20260729",
				"amount": "120.00",
				"details": "Pay & settle <invoice>",
				"payment_type": "NORMAL",
				"source_account_number": "22516020091",
				"source_product_type": "CURRENT_ACCOUNT",
				"beneficiary_name": "Supplier SRL",
				"beneficiary_fiscal_code": "1016606002299",
				"destination_account_number": "MD24TEST0000000000000001",
				"destination_bank_swift_bic": "AGMDMD2X",
				"residency_indicator": "R",
			}
		)
		self.assertIn("<DOCUMENT_NUMBER>PMO-1</DOCUMENT_NUMBER>", xml)
		self.assertIn("<TRANSACTION_AMOUNT>120.00</TRANSACTION_AMOUNT>", xml)
		self.assertIn("Pay &amp; settle &lt;invoice&gt;", xml)
		self.assertIn("<BENEFICIARY_RESIDENCE_INDICATOR>R</BENEFICIARY_RESIDENCE_INDICATOR>", xml)

	@patch("erpnext_moldova_banking.providers.maib.payments.get_access_token")
	@patch("erpnext_moldova_banking.providers.maib.payments.requests.post")
	def test_query_instruction_states_soft_404_sets_local_error(self, mock_post, mock_token):
		enable_maib_settings()
		mock_token.return_value = {
			"access_token": "token",
			"_endpoints": {"api_base_url": "https://example.test"},
		}
		response = MagicMock()
		response.status_code = 404
		response.text = (
			"<Error><ErrorCode>TransfersService.NotFound</ErrorCode>"
			"<ErrorMessage>The requested resource was not found</ErrorMessage></Error>"
		)
		mock_post.return_value = response

		with self.assertRaises(frappe.ValidationError):
			query_instruction_states(["202607280002355"])

		err = getattr(frappe.local, "maib_last_http_error", None)
		self.assertTrue(err)
		self.assertEqual(err.http_status, 404)

	def test_send_rejected_when_outward_disabled(self):
		enable_maib_settings(outward=False, auto_pe=False, api=True)
		with self.assertRaises(frappe.ValidationError) as ctx:
			send_payment_order_to_maib("DOES-NOT-EXIST")
		self.assertIn("disabled", str(ctx.exception).lower())

	def test_parse_and_format_transfer_details(self):
		xml = """<?xml version="1.0" encoding="UTF-8"?>
<root>
    <DocumentNumber>1000000001</DocumentNumber>
    <Date>2026-05-19</Date>
    <CreditTransfer>Mdl</CreditTransfer>
    <PayerName>Sender SRL</PayerName>
    <PayerFiscalCode>1002600000001</PayerFiscalCode>
    <PayerAmount>1000.50</PayerAmount>
    <Currency>MDL</Currency>
    <PayerAccount>2251000000000001</PayerAccount>
    <PayerSubAccount />
    <PayerBank />
    <PayerBankCode>EXMMMD21</PayerBankCode>
    <BeneficiaryName>Recipient SRL</BeneficiaryName>
    <BeneficiaryFiscalCode>1002600000002</BeneficiaryFiscalCode>
    <BeneficiaryAccount>2251000000000002</BeneficiaryAccount>
    <BeneficiarySubAccount />
    <BeneficiaryBank />
    <BeneficiaryBankCode>EXMMMD22</BeneficiaryBankCode>
    <PaymentDestination>Payment for services</PaymentDestination>
    <TransferType>RequiresAction</TransferType>
</root>"""
		details = parse_transfer_details_xml(xml)
		self.assertEqual(details["DocumentNumber"], "1000000001")
		self.assertEqual(details["PayerName"], "Sender SRL")
		self.assertEqual(details["PaymentDestination"], "Payment for services")
		self.assertNotIn("PayerSubAccount", details)

		text = format_transfer_details_description(details)
		self.assertIn("Payment for services", text)
		self.assertIn("Payer: Sender SRL", text)
		self.assertIn("Receiver: Recipient SRL", text)
		self.assertIn("Payer Account: 2251000000000001", text)
		self.assertIn("Receiver Account: 2251000000000002", text)
		self.assertIn("Document Number: 1000000001", text)

		merged = append_transfer_details_to_description("Transaction ID: abc", details)
		self.assertIn("Payer: Sender SRL", merged)
		self.assertIn("Receiver: Recipient SRL", merged)

	@patch("erpnext_moldova_banking.utils.maib_sync.query_transfer_details")
	@patch(
		"erpnext_moldova_banking.utils.maib_sync._company_party_context",
		return_value={"company_iban": "", "company_name": "", "company_idno": ""},
	)
	@patch("erpnext_moldova_banking.utils.maib_sync._is_existing_bank_transaction", return_value=False)
	def test_enrich_new_rows_appends_details(self, _mock_exists, _mock_ctx, mock_query):
		mock_query.return_value = {
			"PayerName": "Sender SRL",
			"BeneficiaryName": "Receiver SRL",
			"PaymentDestination": "Invoice 42",
			"PayerAmount": "100.00",
			"DocumentNumber": "1",
		}
		rows = [
			{
				"date": "2026-07-28",
				"deposit": 100,
				"withdrawal": 0,
				"reference_number": "202607280000001",
				"transaction_id": "202607280000001",
				"payment_destination": "Invoice 42",
				"description": "Invoice 42",
			}
		]
		out = enrich_new_rows_with_transfer_details("BA-1", rows)
		self.assertIn("Payer: Sender SRL", out[0]["description"])
		self.assertIn("Receiver: Receiver SRL", out[0]["description"])
		mock_query.assert_called_once_with("202607280000001", settings=None, soft=True)

	@patch("erpnext_moldova_banking.utils.maib_sync.query_transfer_details")
	@patch("erpnext_moldova_banking.utils.maib_sync._is_existing_bank_transaction", return_value=False)
	def test_enrich_keeps_old_description_when_details_fail(self, _mock_exists, mock_query):
		"""If details-queries fails for TransactionId, keep statement description."""
		mock_query.return_value = {}
		old_desc = (
			"Payment for goods\n\n"
			"Amount: 100.00\n"
			"Currency: MDL\n"
			"Credit/Debit: C\n"
			"Document Number: 42\n"
			"Transaction ID: 213681697437387.000002\n"
			"Counterparty: Supplier SRL\n"
			"Counterparty IDNO: 1002600000002"
		)
		rows = [
			{
				"date": "2026-07-28",
				"deposit": 100,
				"withdrawal": 0,
				"reference_number": "213681697437387.000002",
				"transaction_id": "213681697437387.000002",
				"document_number": "42",
				"description": old_desc,
			}
		]
		out = enrich_new_rows_with_transfer_details("BA-1", rows)
		self.assertEqual(out[0]["description"], old_desc)
		mock_query.assert_called_once_with(
			"213681697437387.000002", settings=None, soft=True
		)
		self.assertFalse(looks_like_transfer_identity("213681697437387.000002"))
		self.assertTrue(looks_like_transfer_identity("202607280000001"))

	@patch("erpnext_moldova_banking.utils.maib_sync.query_transfer_details")
	@patch("erpnext_moldova_banking.utils.maib_sync._is_existing_bank_transaction", return_value=True)
	def test_enrich_skips_existing_transactions(self, _mock_exists, mock_query):
		rows = [
			{
				"date": "2026-07-28",
				"deposit": 100,
				"withdrawal": 0,
				"reference_number": "202607280000001",
				"transaction_id": "202607280000001",
				"description": "Transaction ID: 202607280000001",
			}
		]
		out = enrich_new_rows_with_transfer_details("BA-1", rows)
		self.assertEqual(out[0]["description"], "Transaction ID: 202607280000001")
		mock_query.assert_not_called()

	@patch("erpnext_moldova_banking.providers.maib.payments.get_access_token")
	@patch("erpnext_moldova_banking.providers.maib.payments.requests.post")
	def test_query_transfer_details_soft_400(self, mock_post, mock_token):
		enable_maib_settings()
		frappe.clear_messages()
		mock_token.return_value = {
			"access_token": "token",
			"_endpoints": {"api_base_url": "https://example.test"},
		}
		response = MagicMock()
		response.status_code = 400
		response.text = (
			"<Error><ErrorMessage>Transfer identity must be a valid "
			"transfer ID or instruction ID.</ErrorMessage></Error>"
		)
		mock_post.return_value = response

		details = query_transfer_details("202607280000001", soft=True)
		self.assertEqual(details, {})
		# Soft path must not leave throw messages for the UI.
		self.assertEqual(getattr(frappe.local, "message_log", None) or [], [])

	@patch("erpnext_moldova_banking.providers.maib.payments.get_access_token")
	@patch("erpnext_moldova_banking.providers.maib.payments.requests.post")
	def test_query_transfer_details_posts_transaction_id(self, mock_post, mock_token):
		enable_maib_settings()
		mock_token.return_value = {
			"access_token": "token",
			"_endpoints": {"api_base_url": "https://example.test"},
		}
		response = MagicMock()
		response.status_code = 200
		response.text = (
			"<root><DocumentNumber>1</DocumentNumber><PayerName>A</PayerName></root>"
		)
		mock_post.return_value = response

		details = query_transfer_details("202607280000001", soft=True)
		self.assertEqual(details["DocumentNumber"], "1")
		self.assertEqual(details["PayerName"], "A")
		args, kwargs = mock_post.call_args
		self.assertTrue(args[0].endswith("/api/transfers/details-queries"))
		body = kwargs.get("data") or b""
		if isinstance(body, bytes):
			body = body.decode("utf-8")
		self.assertIn("<TransactionId>202607280000001</TransactionId>", body)
