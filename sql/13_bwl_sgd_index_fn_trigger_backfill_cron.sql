CREATE INDEX IF NOT EXISTS idx_bwl_userkey_date
  ON public.blue_whale_sgd (userkey, "date" DESC);

CREATE OR REPLACE FUNCTION public.fn_bwl_refresh_activity(p_userkey TEXT)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_last_activity DATE;
  v_last_deposit  DATE;
BEGIN
  IF p_userkey IS NULL THEN
    RETURN;
  END IF;

  SELECT MAX(d."date") INTO v_last_activity
  FROM public.blue_whale_sgd d
  WHERE d.userkey = p_userkey;

  SELECT MAX(d."date") INTO v_last_deposit
  FROM public.blue_whale_sgd d
  WHERE d.userkey = p_userkey
    AND COALESCE(d.deposit_cases,0) > 0;

  UPDATE public.blue_whale_sgd
  SET last_activity_days = v_last_activity,
      last_deposit_date  = v_last_deposit,
      days_inactive      = CASE
                             WHEN v_last_deposit IS NULL THEN NULL
                             ELSE (CURRENT_DATE - v_last_deposit)::INT
                           END
  WHERE userkey = p_userkey;
END;
$$;

CREATE OR REPLACE FUNCTION public.trg_bwl_activity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    PERFORM public.fn_bwl_refresh_activity(NEW.userkey);

  ELSIF TG_OP = 'UPDATE' THEN
    PERFORM public.fn_bwl_refresh_activity(NEW.userkey);
    IF NEW.userkey IS DISTINCT FROM OLD.userkey THEN
      PERFORM public.fn_bwl_refresh_activity(OLD.userkey);
    END IF;

  ELSIF TG_OP = 'DELETE' THEN
    PERFORM public.fn_bwl_refresh_activity(OLD.userkey);
  END IF;

  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_bwl_activity ON public.blue_whale_sgd;
CREATE TRIGGER trg_bwl_activity
AFTER INSERT
OR UPDATE OF userkey, "date", deposit_cases
OR DELETE
ON public.blue_whale_sgd
FOR EACH ROW
EXECUTE FUNCTION public.trg_bwl_activity();

WITH k AS (
  SELECT DISTINCT userkey
  FROM public.blue_whale_sgd
  WHERE userkey IS NOT NULL
)
SELECT public.fn_bwl_refresh_activity(userkey) FROM k;

SELECT cron.schedule(
  job_name := 'bwl_days_inactive_from_last_deposit_daily',
  schedule := '5 0 * * *',
  command  := $$
    UPDATE public.blue_whale_sgd
    SET days_inactive = CASE
                          WHEN last_deposit_date IS NULL THEN NULL
                          ELSE (CURRENT_DATE - last_deposit_date)::INT
                        END;
  $$
);
