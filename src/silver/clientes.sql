SELECT
  customer_id                     AS IdCliente,
  customer_zip_code_prefix        AS codigo_postal_cliente,
  customer_city                   AS cidade_cliente,
  customer_state                  AS estado_cliente,
  is_vip                          AS cliente_vip

FROM bronze.olist_ecommerce.customers
