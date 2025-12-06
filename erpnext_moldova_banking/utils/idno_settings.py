import frappe

def get_idno_fields():
    """
    Return dict with configured IDNO fields for Company, Customer, Supplier.
    Example:
    {
        "company": "tax_id",
        "customer": "idno",
        "supplier": "idno"
    }
    """
    settings = frappe.get_single("Moldova Banking Settings")

    return {
        "company": settings.company_idno_field or "tax_id",
        "customer": settings.customer_idno_field or "tax_id",
        "supplier": settings.supplier_idno_field or "tax_id",
    }
