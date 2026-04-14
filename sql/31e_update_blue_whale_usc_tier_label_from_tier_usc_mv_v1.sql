UPDATE public.blue_whale_usc b
SET tier_label = m.tier_name
FROM public.tier_usc_mv_v1 m
WHERE b.userkey = m.userkey
  AND b.line    = m.line;
