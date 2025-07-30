SELECT
  IdTransacao                     AS IdTransaction,
  IdCliente                       AS IdCustomer,
  DtCriacao                       AS DateCreate,
  QtdePontos                      AS AmountPoints,
  DescSistemaOrigem               AS SystemOccurredTransaction


FROM bronze.upsell.transacoes