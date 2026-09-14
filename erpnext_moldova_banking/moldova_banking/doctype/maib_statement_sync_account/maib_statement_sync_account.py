# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from frappe.model.document import Document


class MAIBStatementSyncAccount(Document):
	def validate(self):
		from erpnext_moldova_banking.utils.maib_sync import validate_sync_hours

		validate_sync_hours(self.hours_from, self.hours_to)
