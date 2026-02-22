-- Populate update_unique_code with latest unique_code per user_unique (blue_whale_sgd)
WITH latest AS (
  SELECT user_unique, unique_code AS latest_unique_code
  FROM (
    SELECT user_unique, unique_code, "date",
      ROW_NUMBER() OVER (PARTITION BY user_unique ORDER BY "date" DESC, ctid DESC) AS rn
    FROM public.blue_whale_sgd
  ) t
  WHERE rn = 1
)
UPDATE public.blue_whale_sgd AS b
SET update_unique_code = l.latest_unique_code
FROM latest AS l
WHERE b.user_unique = l.user_unique;
