UPDATE public.blue_whale_sgd AS m
SET
    register_date      = COALESCE(m.register_date,      nd.register_date),
    first_deposit_date = COALESCE(m.first_deposit_date, nd.first_deposit_date),
    first_deposit_amount = COALESCE(m.first_deposit_amount, nd.first_deposit_amount),
    traffic            = COALESCE(m.traffic,            nd.traffic)
FROM public.rs_blue_whale_sgd AS nd
WHERE nd.user_unique = m.user_unique
  AND (
      m.register_date IS NULL
      OR m.first_deposit_date IS NULL
      OR m.first_deposit_amount IS NULL
      OR m.traffic IS NULL
  );
