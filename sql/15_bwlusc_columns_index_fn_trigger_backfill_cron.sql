ALTER TABLE public.blue_whale_usc
  ADD COLUMN IF NOT EXISTS last_activity_days DATE,
  ADD COLUMN IF NOT EXISTS last_deposit_date  DATE,
  ADD COLUMN IF NOT EXISTS days_inactive      INT;

CREATE INDEX IF NOT EXISTS idx_bwlusc_userkey_date
  ON public.blue_whale_usc (userkey, "date" DESC);

CREATE OR REPLACE FUNCTION public.fn_bwlusc_refresh_activity(p_userkey TEXT)
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
  FROM public.blue_whale_usc d
  WHERE d.userkey = p_userkey;

  SELECT MAX(d."date") INTO v_last_deposit
  FROM public.blue_whale_usc d
  WHERE d.userkey = p_userkey
    AND COALESCE(d.deposit_cases,0) > 0;

  UPDATE public.blue_whale_usc
  SET last_activity_days = v_last_activity,
      last_deposit_date  = v_last_deposit,
      days_inactive      = CASE
                             WHEN v_last_deposit IS NULL THEN NULL
                             ELSE (CURRENT_DATE - v_last_deposit)::INT
                           END
  WHERE userkey = p_userkey;
END;
$$;

CREATE OR REPLACE FUNCTION public.trg_bwlusc_activity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    PERFORM public.fn_bwlusc_refresh_activity(NEW.userkey);

  ELSIF TG_OP = 'UPDATE' THEN
    PERFORM public.fn_bwlusc_refresh_activity(NEW.userkey);
    IF NEW.userkey IS DISTINCT FROM OLD.userkey THEN
      PERFORM public.fn_bwlusc_refresh_activity(OLD.userkey);
    END IF;

  ELSIF TG_OP = 'DELETE' THEN
    PERFORM public.fn_bwlusc_refresh_activity(OLD.userkey);
  END IF;

  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_bwlusc_activity ON public.blue_whale_usc;
CREATE TRIGGER trg_bwlusc_activity
AFTER INSERT
OR UPDATE OF userkey, "date", deposit_cases
OR DELETE
ON public.blue_whale_usc
FOR EACH ROW
EXECUTE FUNCTION public.trg_bwlusc_activity();

WITH k AS (
  SELECT DISTINCT userkey
  FROM public.blue_whale_usc
  WHERE userkey IS NOT NULL
)
SELECT public.fn_bwlusc_refresh_activity(userkey) FROM k;

SELECT cron.schedule(
  job_name := 'bwlusc_days_inactive_from_last_deposit_daily',
  schedule := '5 0 * * *',
  command  := $$
    UPDATE public.blue_whale_usc
    SET days_inactive = CASE
                          WHEN last_deposit_date IS NULL THEN NULL
                          ELSE (CURRENT_DATE - last_deposit_date)::INT
                        END;
  $$
);
