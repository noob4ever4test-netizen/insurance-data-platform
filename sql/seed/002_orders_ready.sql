SELECT user_id, session_id,
    dictHas('core.purchase_orders_dict', (user_id, session_id))
    AND dictGet('core.purchase_orders_dict',
        ('order_id', 'order_created_at', 'product_type', 'partner_id', 'national_id', 'phone',
         'policy_no', 'order_premium_amount', 'product_attributes_json', 'financial_created_at',
         'payment_status', 'paid_amount'), (user_id, session_id))
        = (order_id, order_created_at, product_type, partner_id, national_id, phone,
           policy_no, order_premium_amount, product_attributes_json, financial_created_at,
           payment_status, paid_amount) AS ready
FROM core.purchase_order_lookup;
