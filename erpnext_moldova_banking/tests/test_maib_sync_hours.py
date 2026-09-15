# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from datetime import datetime
from types import SimpleNamespace

import frappe
from frappe.tests.utils import FrappeTestCase

from erpnext_moldova_banking.utils.maib_sync import (
	_is_due,
	is_within_global_active_hours,
	validate_active_hours_rows,
)


class TestMaibSyncDue(FrappeTestCase):
	def test_due_respects_schedule(self):
		row = SimpleNamespace(
			disabled=0,
			schedule="Every 5 minutes",
			last_synced_on=None,
		)
		self.assertTrue(_is_due(row, datetime(2026, 9, 15, 20, 0)))

	def test_manual_only_is_never_due(self):
		row = SimpleNamespace(
			disabled=0,
			schedule="Manual only",
			last_synced_on=None,
		)
		self.assertFalse(_is_due(row, datetime(2026, 9, 15, 10, 0)))


class TestGlobalActiveHours(FrappeTestCase):
	def test_disabled_never_blocks(self):
		settings = SimpleNamespace(enable_active_hours=0, active_hours=[])
		self.assertTrue(is_within_global_active_hours(settings, datetime(2026, 9, 15, 3, 0)))

	def test_enabled_without_rows_blocks(self):
		settings = SimpleNamespace(enable_active_hours=1, active_hours=[])
		self.assertFalse(is_within_global_active_hours(settings, datetime(2026, 9, 15, 10, 0)))

	def test_same_day_window(self):
		# 2026-09-15 is a Tuesday.
		row = SimpleNamespace(day_of_week="Tuesday", time_from="09:00:00", time_to="17:00:00")
		settings = SimpleNamespace(enable_active_hours=1, active_hours=[row])
		self.assertTrue(is_within_global_active_hours(settings, datetime(2026, 9, 15, 9, 0)))
		self.assertTrue(is_within_global_active_hours(settings, datetime(2026, 9, 15, 17, 0)))
		self.assertFalse(is_within_global_active_hours(settings, datetime(2026, 9, 15, 8, 59)))
		self.assertFalse(is_within_global_active_hours(settings, datetime(2026, 9, 15, 17, 1)))
		self.assertFalse(is_within_global_active_hours(settings, datetime(2026, 9, 14, 10, 0)))

	def test_overnight_window(self):
		row = SimpleNamespace(day_of_week="Tuesday", time_from="22:00:00", time_to="06:00:00")
		settings = SimpleNamespace(enable_active_hours=1, active_hours=[row])
		self.assertTrue(is_within_global_active_hours(settings, datetime(2026, 9, 15, 22, 0)))
		self.assertTrue(is_within_global_active_hours(settings, datetime(2026, 9, 15, 6, 0)))
		self.assertFalse(is_within_global_active_hours(settings, datetime(2026, 9, 15, 7, 0)))
		self.assertFalse(is_within_global_active_hours(settings, datetime(2026, 9, 15, 21, 0)))

	def test_validate_requires_a_period(self):
		with self.assertRaises(frappe.ValidationError):
			validate_active_hours_rows([])

	def test_validate_rejects_equal_times(self):
		row = SimpleNamespace(day_of_week="Monday", time_from="10:00:00", time_to="10:00:00")
		with self.assertRaises(frappe.ValidationError):
			validate_active_hours_rows([row])

	def test_validate_allows_overnight(self):
		row = SimpleNamespace(day_of_week="Monday", time_from="22:00:00", time_to="06:00:00")
		self.assertIsNone(validate_active_hours_rows([row]))
