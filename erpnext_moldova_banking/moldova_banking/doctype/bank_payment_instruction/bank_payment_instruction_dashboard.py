from frappe import _


def get_data():
	return {
		"fieldname": "bank_payment_instruction",
		"internal_links": {
			"Purchase Invoice": ["invoices", "purchase_invoice"],
			"Payment Entry": "payment_entry",
			"Bank Transaction": "linked_bank_transaction",
		},
		"transactions": [
			{"label": _("Reference"), "items": ["Purchase Invoice"]},
			{"label": _("Payment"), "items": ["Payment Entry", "Bank Transaction"]},
		],
	}
