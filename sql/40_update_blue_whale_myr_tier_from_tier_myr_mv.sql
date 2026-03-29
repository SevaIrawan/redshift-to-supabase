UPDATE public.blue_whale_myr b
SET tier_label = m.tier_name
FROM public.tier_myr_mv_v1 m
WHERE b.userkey = m.userkey
  AND b.year    = m.year
  AND b.month   = m.month
  AND b.line    = m.line;
