# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from unittest.mock import MagicMock, patch

import frappe
from frappe.tests.utils import FrappeTestCase

from erpnext_moldova_banking.providers.maib.client import (
	_normalize_company_name,
	get_access_token,
	get_company_maib_settings,
	get_maib_defaults,
	resolve_maib_endpoints,
)
from erpnext_moldova_banking.providers.maib.payments import (
	append_transfer_details_to_description,
	build_ordinary_payment_xml,
	create_ordinary_payment,
	format_transfer_details_description,
	looks_like_transfer_identity,
	map_maib_status,
	parse_transfer_details_xml,
	query_instruction_states,
	query_transfer_details,
)
from erpnext_moldova_banking.tests.utils import enable_maib_settings
from erpnext_moldova_banking.utils.bank_payment_instruction import (
	_next_document_number,
	make_document_number,
	refresh_instruction_status,
	send_instruction_to_maib,
)
from erpnext_moldova_banking.utils.maib_sync import (
	_normalize_company_selection,
	enrich_new_rows_with_transfer_details,
)


class TestMaibPaymentsClient(FrappeTestCase):
	def tearDown(self):
		frappe.db.rollback()

	def test_status_map(self):
		self.assertEqual(map_maib_status("RequiresAction"), "Waiting For Authorisation")
		self.assertEqual(map_maib_status("Succeeded"), "Executed")
		self.assertEqual(map_maib_status("Rejected"), "Rejected")
		self.assertEqual(map_maib_status(""), "API Error")
		self.assertEqual(map_maib_status("SomethingNew"), "In Process")

	def test_legacy_test_token_url_is_upgraded_at_runtime(self):
		settings = frappe._dict(
			maib_environment="Test",
			maib_token_url="https://test-business-sso.maib.md/api/connect/token",
			maib_api_base_url="",
			maib_scope="",
		)
		endpoints = resolve_maib_endpoints(settings)
		self.assertEqual(
			endpoints["token_url"],
			"https://test-business-sso.maib.md/connect/token",
		)

	def test_production_defaults_use_live_hosts(self):
		defaults = get_maib_defaults("Production")
		self.assertEqual(defaults["token_url"], "https://business-sso.maib.md/connect/token")
		self.assertEqual(defaults["api_base_url"], "https://business-api.maib.md")
		self.assertEqual(defaults["scope"], "payments_gateway")

		endpoints = resolve_maib_endpoints(
			frappe._dict(
				maib_environment="Production",
				maib_token_url="",
				maib_api_base_url="",
				maib_scope="",
			)
		)
		self.assertEqual(endpoints["token_url"], defaults["token_url"])
		self.assertEqual(endpoints["api_base_url"], defaults["api_base_url"])
		self.assertEqual(endpoints["scope"], defaults["scope"])

	def test_company_values_are_normalized_from_frappe_payloads(self):
		self.assertEqual(_normalize_company_name('["Best Test SRL"]'), "Best Test SRL")
		self.assertEqual(_normalize_company_name([" Best Test SRL "]), "Best Test SRL")
		self.assertEqual(
			_normalize_company_selection('["Best Test SRL", "Second SRL"]'),
			["Best Test SRL", "Second SRL"],
		)
		self.assertEqual(
			_normalize_company_selection("Best Test SRL, Second SRL"),
			["Best Test SRL", "Second SRL"],
		)

	@patch("erpnext_moldova_banking.providers.maib.client.frappe.get_single")
	def test_company_credentials_match_json_array_selection(self, mock_get_single):
		first = frappe._dict(company="Best Test SRL", client_id="first")
		second = frappe._dict(company="Second SRL", client_id="second")
		mock_get_single.return_value = frappe._dict(
			maib_company_settings=[first, second]
		)
		self.assertIs(get_company_maib_settings('["Second SRL"]'), second)

	@patch("erpnext_moldova_banking.providers.maib.client.requests.post")
	@patch("erpnext_moldova_banking.providers.maib.client.get_maib_client_secret")
	@patch("erpnext_moldova_banking.providers.maib.client.get_company_maib_settings")
	@patch("erpnext_moldova_banking.providers.maib.client.get_maib_settings")
	def test_access_token_uses_selected_company_credentials(
		self, mock_settings, mock_company_settings, mock_secret, mock_post
	):
		mock_settings.return_value = frappe._dict(
			maib_enabled=1,
			maib_environment="Test",
			maib_token_url="https://example.test/token",
			maib_api_base_url="https://example.test",
			maib_scope="payments_gateway",
		)
		mock_company_settings.return_value = frappe._dict(client_id="company-client")
		mock_secret.return_value = "company-secret"
		mock_post.return_value = MagicMock(
			status_code=200,
			json=lambda: {"access_token": "token", "token_type": "Bearer"},
		)

		payload = get_access_token(company='["Best Test SRL"]')

		mock_company_settings.assert_called_once_with("Best Test SRL")
		mock_secret.assert_called_once_with("Best Test SRL")
		self.assertEqual(payload["access_token"], "token")
		self.assertEqual(payload["_environment"], "Test")
		self.assertEqual(
			mock_post.call_args.kwargs["data"],
			{
				"grant_type": "client_credentials",
				"client_id": "company-client",
				"client_secret": "company-secret",
				"scope": "payments_gateway",
			},
		)

	@patch("erpnext_moldova_banking.providers.maib.client.get_maib_settings")
	def test_access_token_rejects_missing_company_credentials(self, mock_settings):
		mock_settings.return_value = frappe._dict(maib_enabled=1, maib_environment="Test")
		with patch(
			"erpnext_moldova_banking.providers.maib.client.get_company_maib_settings",
			return_value=None,
		):
			with self.assertRaises(frappe.ValidationError) as ctx:
				get_access_token(company="Missing SRL")
		self.assertIn("Missing SRL", str(ctx.exception))

	def test_maib_external_id_is_stable_and_numeric(self):
		doc = frappe._dict(name="BPI-2026-00001", company="_Test Company")
		first = make_document_number(doc)
		second = make_document_number(doc)
		self.assertEqual(first, second)
		self.assertEqual(first, doc.document_number)
		self.assertTrue(first.isdigit())
		self.assertLessEqual(len(first), 9)

	@patch("erpnext_moldova_banking.utils.bank_payment_instruction.frappe.db.sql")
	def test_next_document_number_increments_and_skips_legacy_hashes(self, mock_sql):
		mock_sql.return_value = [("7",), ("814",), ("131084521085327",)]
		self.assertEqual(_next_document_number("_Test Company"), "815")
		mock_sql.return_value = []
		self.assertEqual(_next_document_number("_Test Company"), "1")

	def test_explicit_maib_external_id_must_be_numeric(self):
		doc = frappe._dict(
			name="BPI-2026-00001",
			company="_Test Company",
			document_number="PMO-1",
		)
		with self.assertRaises(frappe.ValidationError):
			make_document_number(doc)

	def test_explicit_maib_external_id_is_preserved(self):
		doc = frappe._dict(
			name="BPI-2026-00001",
			company="_Test Company",
			document_number="202608300004776",
		)
		self.assertEqual(make_document_number(doc), "202608300004776")

	def test_document_number_requires_saved_instruction(self):
		with self.assertRaises(frappe.ValidationError):
			make_document_number(frappe._dict(company="_Test Company"))

	def test_build_ordinary_payment_xml(self):
		xml = build_ordinary_payment_xml(
			{
				"document_number": "PMO-1",
				"payment_date": "20260729",
				"amount": "120.00",
				"details": "Pay & settle <invoice>",
				"payment_type": "NORMAL",
				"source_account_number": "22516020091",
				"source_product_type": "Operational",
				"beneficiary_name": "Supplier SRL",
				"beneficiary_fiscal_code": "1016606002299",
				"destination_account_number": "MD24TEST0000000000000001",
				"destination_bank_swift_bic": "AGMDMD2X",
				"residency_indicator": "R",
			}
		)
		self.assertIn("<DOCUMENT_NUMBER>PMO-1</DOCUMENT_NUMBER>", xml)
		self.assertIn("<TRANSACTION_AMOUNT>120.00</TRANSACTION_AMOUNT>", xml)
		self.assertIn("<SOURCE_PRODUCT_TYPE>Operational</SOURCE_PRODUCT_TYPE>", xml)
		self.assertIn("Pay &amp; settle &lt;invoice&gt;", xml)
		self.assertIn("<BENEFICIARY_RESIDENCE_INDICATOR>R</BENEFICIARY_RESIDENCE_INDICATOR>", xml)

	@patch("erpnext_moldova_banking.providers.maib.payments._post_xml")
	def test_create_payment_forwards_company_to_maib_token(self, mock_post):
		mock_post.return_value = "<root><INSTRUCTION_ID>202608300004776</INSTRUCTION_ID></root>"
		result = create_ordinary_payment(
			{
				"document_number": "202608300004776",
				"payment_date": "20260830",
				"amount": "10.00",
				"details": "Invoice",
				"source_account_number": "22516020091",
				"beneficiary_name": "Supplier SRL",
				"beneficiary_fiscal_code": "1002600015382",
				"destination_account_number": "MD24TEST0000000000000001",
				"destination_bank_swift_bic": "AGMDMD2X",
				"residency_indicator": "R",
			},
			company="Best Test SRL",
		)
		self.assertEqual(result["instruction_id"], "202608300004776")
		self.assertEqual(mock_post.call_args.kwargs["company"], "Best Test SRL")

	@patch("erpnext_moldova_banking.utils.bank_payment_instruction._set_instruction_fields")
	@patch("erpnext_moldova_banking.utils.bank_payment_instruction.query_instruction_states")
	@patch("erpnext_moldova_banking.utils.bank_payment_instruction.frappe.get_doc")
	@patch("erpnext_moldova_banking.utils.bank_payment_instruction.frappe.has_permission")
	def test_refresh_status_queries_instruction_for_company(
		self, _mock_permission, mock_get_doc, mock_query, mock_set_fields
	):
		doc = MagicMock()
		doc.name = "BPI-0001"
		doc.company = "Best Test SRL"
		doc.get.side_effect = lambda key: {
			"bank_instruction_id": "202608300004776",
			"status": "Executed",
			"bank_comment": "accepted",
		}.get(key)
		mock_get_doc.return_value = doc
		mock_query.return_value = [
			{"status": "Succeeded", "comment": "accepted", "processing_date": "20260830"}
		]

		with patch(
			"erpnext_moldova_banking.utils.maib_payment_match.try_match_after_status_update",
			return_value={"ok": True},
		):
			result = refresh_instruction_status(doc.name)

		mock_query.assert_called_once_with(["202608300004776"], company="Best Test SRL")
		self.assertEqual(mock_set_fields.call_args.args[1]["status"], "Executed")
		self.assertEqual(result["raw_status"], "Succeeded")
		self.assertEqual(result["payment_match"], {"ok": True})

	@patch("erpnext_moldova_banking.utils.bank_payment_instruction._set_instruction_fields")
	@patch("erpnext_moldova_banking.utils.bank_payment_instruction.query_instruction_states")
	@patch("erpnext_moldova_banking.utils.bank_payment_instruction.frappe.get_doc")
	@patch("erpnext_moldova_banking.utils.bank_payment_instruction.frappe.has_permission")
	def test_refresh_status_keeps_current_status_on_maib_404(
		self, _mock_permission, mock_get_doc, mock_query, mock_set_fields
	):
		doc = MagicMock()
		doc.name = "BPI-0001"
		doc.company = "Best Test SRL"
		doc.get.side_effect = lambda key: {
			"bank_instruction_id": "202608300004776",
			"status": "Waiting For Authorisation",
		}.get(key)
		mock_get_doc.return_value = doc
		mock_query.side_effect = frappe.ValidationError("TransfersService.NotFound")
		frappe.local.maib_last_http_error = frappe._dict(http_status=404)

		result = refresh_instruction_status(doc.name)

		mock_query.assert_called_once_with(["202608300004776"], company="Best Test SRL")
		self.assertNotIn("status", mock_set_fields.call_args.args[1])
		self.assertTrue(result["soft_error"])
		del frappe.local.maib_last_http_error

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
			send_instruction_to_maib("DOES-NOT-EXIST")
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
		desc = out[0]["description"]
		self.assertIn("Invoice 42", desc.split("\n", 1)[0])
		self.assertIn("Amount: 100.00", desc)
		self.assertIn("Document Number: 1", desc)
		self.assertIn("Date Written: 28.07.2026", desc)
		self.assertIn("Payer: Sender SRL", desc)
		self.assertIn("Receiver: Receiver SRL", desc)
		self.assertGreater(desc.index("Payer: Sender SRL"), desc.index("Date Written:"))
		self.assertGreater(desc.index("Receiver: Receiver SRL"), desc.index("Payer: Sender SRL"))
		self.assertGreater(desc.index("Transaction ID:"), desc.index("Receiver: Receiver SRL"))
		mock_query.assert_called_once_with(
			"202607280000001", settings=None, company="", soft=True
		)

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
			"213681697437387.000002", settings=None, company="", soft=True
		)
		self.assertFalse(looks_like_transfer_identity("213681697437387.000002"))
		self.assertTrue(looks_like_transfer_identity("202607280000001"))
		self.assertTrue(looks_like_transfer_identity("d6b8e7ad-67c7-4940-8787-ac402254d019"))

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
