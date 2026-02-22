UPDATE public.blue_whale_usc
SET squad_lead = get_squad_lead_from_line(line)
WHERE squad_lead IS NULL OR squad_lead != get_squad_lead_from_line(line);
