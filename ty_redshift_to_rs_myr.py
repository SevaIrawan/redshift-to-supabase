"""TY: Redshift → rs (MYR). Pakai sync_config_myr.json / SYNC_CONFIG_MYR_PATH."""
from ty_redshift_to_rs_engine import run_ty_main

if __name__ == "__main__":
    run_ty_main(env_var="SYNC_CONFIG_MYR_PATH", default_config="sync_config_myr.json", market="myr")
