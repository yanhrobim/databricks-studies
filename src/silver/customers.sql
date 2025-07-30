SELECT
  IdCliente                         AS IdCustomer,
  FlEmail                           AS HasEmail,
  FlTwitch                          AS UsesTwitch,
  FlYoutube                         AS UsesYoutube,
  FlBlueSky                         AS UsesBlueSky,
  Flinstagram                       AS HasInstagram,
  QtdePontos                        AS AmountPoints,
  DtCriacao                         AS DateCreate,
  DtAtualizacao                     AS DateUpdate

FROM bronze.upsell.clientes
