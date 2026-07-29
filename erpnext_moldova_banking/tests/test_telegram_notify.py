# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from unittest.mock import MagicMock, patch

import frappe
from frappe.tests.utils import FrappeTestCase

from erpnext_moldova_banking.utils.telegram_notify import (
	_should_notify,
	format_bank_transaction_message,
	notify_new_bank_transaction,
)


def _settings(**overrides):
	s = frappe._dict(
		{
			"telegram_enabled": 1,
			"telegram_chat_id": "-1001",
			"telegram_notify_file_import": 1,
			"telegram_notify_api": 1,
			"telegram_notify_incoming": 1,
			"telegram_notify_outgoing": 1,
			"telegram_notify_automation_matched": 1,
			"telegram_notify_automation_unmatched": 1,
			"telegram_field_company": 1,
			"telegram_field_party_name": 1,
			"telegram_field_party_idno": 1,
			"telegram_field_amount": 1,
			"telegram_field_description": 1,
			"telegram_field_date": 1,
		}
	)
	s.update(overrides)
	return s


class TestTelegramNotify(FrappeTestCase):
	@patch("erpnext_moldova_banking.utils.telegram_notify._get_bot_token", return_value="token")
	def test_should_notify_filters(self, _token):
		settings = _settings()
		self.assertTrue(
			_should_notify(settings, source="api", incoming=True, automation_matched=False)
		)
		self.assertFalse(
			_should_notify(
				_settings(telegram_notify_api=0),
				source="api",
				incoming=True,
				automation_matched=False,
			)
		)
		self.assertFalse(
			_should_notify(
				_settings(telegram_notify_incoming=0),
				source="api",
				incoming=True,
				automation_matched=False,
			)
		)
		self.assertFalse(
			_should_notify(
				_settings(telegram_notify_outgoing=0),
				source="file_import",
				incoming=False,
				automation_matched=True,
			)
		)
		self.assertFalse(
			_should_notify(
				_settings(telegram_notify_automation_matched=0),
				source="api",
				incoming=True,
				automation_matched=True,
			)
		)
		self.assertTrue(
			_should_notify(
				_settings(telegram_notify_automation_matched=0),
				source="api",
				incoming=True,
				automation_matched=False,
			)
		)

	def test_format_respects_message_fields(self):
		doc = frappe._dict(
			name="ACC-BTN-1",
			company="Best Test SRL",
			date="2026-07-28",
			deposit=100,
			withdrawal=0,
			currency="MDL",
			description="Payment\n\nCounterparty: Supplier SRL\nCounterparty IDNO: 123",
			bank_party_name="",
			party="",
		)
		text = format_bank_transaction_message(
			doc,
			source="api",
			automation_matched=False,
			settings=_settings(
				telegram_field_company=0,
				telegram_field_description=0,
				telegram_field_party_idno=0,
			),
		)
		self.assertIn("Incoming Bank Transaction ACC-BTN-1", text)
		self.assertIn("Amount: +100.00 MDL", text)
		self.assertIn("Sender: Supplier SRL", text)
		self.assertNotIn("Company:", text)
		self.assertNotIn("IDNO/IDNP", text)
		self.assertNotIn("Payment\n", text)

	@patch("erpnext_moldova_banking.utils.telegram_notify.send_telegram_message")
	@patch("erpnext_moldova_banking.utils.telegram_notify._get_settings")
	@patch("erpnext_moldova_banking.utils.telegram_notify._get_bot_token", return_value="token")
	def test_notify_sends_when_allowed(self, _token, mock_settings, mock_send):
		mock_settings.return_value = _settings()
		doc = frappe._dict(
			name="ACC-BTN-2",
			company="Best Test SRL",
			date="2026-07-28",
			deposit=0,
			withdrawal=50,
			currency="MDL",
			description="Fee",
			bank_party_name="Bank",
			party="",
		)
		ok = notify_new_bank_transaction(doc, source="api", automation_matched=False)
		self.assertTrue(ok)
		mock_send.assert_called_once()
		sent = mock_send.call_args[0][0]
		self.assertIn("Amount: -50.00 MDL", sent)

	@patch("erpnext_moldova_banking.utils.telegram_notify.send_telegram_message")
	@patch("erpnext_moldova_banking.utils.telegram_notify._get_settings")
	@patch("erpnext_moldova_banking.utils.telegram_notify._get_bot_token", return_value="token")
	def test_notify_soft_fails(self, _token, mock_settings, mock_send):
		mock_settings.return_value = _settings()
		mock_send.side_effect = frappe.ValidationError("boom")
		doc = frappe._dict(
			name="ACC-BTN-3",
			company="Best Test SRL",
			date="2026-07-28",
			deposit=10,
			withdrawal=0,
			currency="MDL",
			description="X",
			bank_party_name="",
			party="",
		)
		ok = notify_new_bank_transaction(doc, source="api", automation_matched=False)
		self.assertFalse(ok)
