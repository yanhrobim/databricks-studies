SELECT
  IdTransacaoProduto                AS IdProductTransaction,      
  IdTransacao                       AS IdTransaction,
  IdProduto                         AS IdProduct,
  QtdeProduto                       AS AmountProduct,
  VlProduto                         AS ValueProduct


FROM bronze.upsell.transacao_produto