# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.model.document import Document

# Duplicate copies these fields. Amend copies them even when no_copy is set.
_BANK_RESPONSE_FIELDS = (
	"document_number",
	"bank_instruction_id",
	"last_sync",
	"bank_comment",
	"api_error",
	"payment_entry",
	"linked_bank_transaction",
)


class BankPaymentInstruction(Document):
	def before_insert(self):
		self.bank_status = "Not Sent"
		for fieldname in _BANK_RESPONSE_FIELDS:
			self.set(fieldname, None)

	def validate(self):
		from erpnext_moldova_banking.utils.bank_payment_instruction import prepare_instruction

		prepare_instruction(self)

	def before_cancel(self):
		status = self.bank_status or "Not Sent"
		if status in ("Rejected", "Canceled"):
			return
		frappe.throw(
			_(
				"Bank Payment Instruction {0} cannot be cancelled in the current bank status ({1}). "
				"Cancellation is allowed only for Rejected and Canceled. "
				"Cancel the payment in the bank interface first."
			).format(self.name, status),
			title=_("Cannot Cancel"),
		)
