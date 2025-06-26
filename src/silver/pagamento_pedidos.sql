SELECT
          order_id                  AS IdPedido,
          payment_sequential        AS sequencia_pagamento,
          payment_type              AS tipo_pagamento,
          payment_installments      AS parcelas_pagamento,
          payment_value             AS valor_pagamento

FROM bronze.olist_ecommerce.order_payments