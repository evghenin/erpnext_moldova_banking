# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from datetime import datetime
from types import SimpleNamespace

import frappe
from frappe.tests.utils import FrappeTestCase

from erpnext_moldova_banking.utils.maib_sync import (
	_is_due,
	_within_sync_hours,
	validate_sync_hours,
)


class TestMaibSyncHours(FrappeTestCase):
	def test_both_empty_is_allowed(self):
		self.assertIsNone(validate_sync_hours(None, None))
		self.assertIsNone(validate_sync_hours("", ""))

	def test_one_side_only_is_rejected(self):
		with self.assertRaises(frappe.ValidationError):
			validate_sync_hours(9, None)
		with self.assertRaises(frappe.ValidationError):
			validate_sync_hours(None, 17)

	def test_hours_must_be_0_to_23(self):
		with self.assertRaises(frappe.ValidationError):
			validate_sync_hours(-1, 10)
		with self.assertRaises(frappe.ValidationError):
			validate_sync_hours(9, 24)

	def test_zero_is_a_valid_hour(self):
		self.assertEqual(validate_sync_hours(0, 6), (0, 6))

	def test_window_inclusive(self):
		row = SimpleNamespace(hours_from=9, hours_to=17)
		self.assertTrue(_within_sync_hours(row, datetime(2026, 9, 15, 9, 0)))
		self.assertTrue(_within_sync_hours(row, datetime(2026, 9, 15, 17, 59)))
		self.assertFalse(_within_sync_hours(row, datetime(2026, 9, 15, 8, 59)))
		self.assertFalse(_within_sync_hours(row, datetime(2026, 9, 15, 18, 0)))

	def test_overnight_window(self):
		row = SimpleNamespace(hours_from=22, hours_to=6)
		self.assertTrue(_within_sync_hours(row, datetime(2026, 9, 15, 22, 0)))
		self.assertTrue(_within_sync_hours(row, datetime(2026, 9, 15, 6, 30)))
		self.assertFalse(_within_sync_hours(row, datetime(2026, 9, 15, 7, 0)))
		self.assertFalse(_within_sync_hours(row, datetime(2026, 9, 15, 21, 0)))

	def test_empty_window_never_blocks(self):
		row = SimpleNamespace(hours_from=None, hours_to=None)
		self.assertTrue(_within_sync_hours(row, datetime(2026, 9, 15, 3, 0)))

	def test_due_respects_hours(self):
		row = SimpleNamespace(
			disabled=0,
			schedule="Every 5 minutes",
			last_synced_on=None,
			hours_from=9,
			hours_to=17,
		)
		self.assertTrue(_is_due(row, datetime(2026, 9, 15, 10, 0)))
		self.assertFalse(_is_due(row, datetime(2026, 9, 15, 20, 0)))
