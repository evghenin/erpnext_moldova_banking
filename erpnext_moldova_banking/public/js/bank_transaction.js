frappe.realtime.on("bank_transaction_reload", (data) => {
    if (
        cur_frm &&
        cur_frm.doctype === data.doctype &&
        cur_frm.doc.name === data.name
    ) {
        cur_frm.reload_doc();
    }
});