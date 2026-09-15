# Copyright (c) 2025, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from frappe.model.document import Document


class MoldovaBankingSettings(Document):
	def validate(self):
		if not self.enable_active_hours:
			return

		from erpnext_moldova_banking.utils.maib_sync import validate_active_hours_rows

		validate_active_hours_rows(self.get("active_hours") or [])
