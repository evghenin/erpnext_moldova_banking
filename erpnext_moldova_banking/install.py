# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from erpnext_moldova_banking.utils.residency import ensure_moldova_residency_status_fields


def after_install():
	ensure_moldova_residency_status_fields()


def after_migrate():
	ensure_moldova_residency_status_fields()
