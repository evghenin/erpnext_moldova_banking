# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from frappe.model.document import Document


class BankPaymentInstruction(Document):
	def validate(self):
		from erpnext_moldova_banking.utils.bank_payment_instruction import prepare_instruction

		prepare_instruction(self)
