from frappe import _


def get_purchase_invoice_dashboard(data):
	data = data or {}
	data.setdefault("transactions", [])

	for group in data["transactions"]:
		if group.get("label") in (_("Payment"), "Payment"):
			items = group.setdefault("items", [])
			if "Bank Payment Instruction" not in items:
				items.append("Bank Payment Instruction")
			return data

	data["transactions"].append({"label": _("Payment"), "items": ["Bank Payment Instruction"]})
	return data
