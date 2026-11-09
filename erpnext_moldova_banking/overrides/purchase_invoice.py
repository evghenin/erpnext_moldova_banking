# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

"""Purchase Invoice hooks for Moldova banking."""

# Linked from the BPI child table. Frappe reports the parent doctype in the error.
_BPI_LINK_DOCTYPES = (
	"Bank Payment Instruction",
	"Bank Payment Instruction Invoice",
)


def ignore_bank_payment_instruction_on_cancel(doc, method=None):
	"""Let a Purchase Invoice be cancelled while it stays on a Bank Payment Instruction.

	``ignore_linked_doctypes`` is applied only when the method is Cancel, so delete
	stays blocked. The Bank Payment Instruction is not loaded or updated.
	"""
	ignored = list(doc.get("ignore_linked_doctypes") or [])
	for doctype in _BPI_LINK_DOCTYPES:
		if doctype not in ignored:
			ignored.append(doctype)
	doc.ignore_linked_doctypes = ignored
