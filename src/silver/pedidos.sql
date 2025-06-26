SELECT

      order_id                          AS IdPedido,
      customer_id                       AS IdCliente,
      order_status                      AS status_pedido,
      order_purchase_timestamp          AS data_de_compra,
      order_approved_at                 AS pedido_aprovado_em,
      order_delivered_carrier_date      AS data_de_entrega_do_pedido_a_transportadora,
      order_delivered_customer_date     AS data_de_entrega_do_pedido_ao_cliente,
      order_estimated_delivery_date     AS data_estimada_de_entrega_pedido

FROM bronze.olist_ecommerce.orders