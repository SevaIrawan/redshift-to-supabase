"""TY: Redshift → rs (USC). Pakai sync_config.json / SYNC_CONFIG_PATH."""
from ty_redshift_to_rs_engine import run_ty_main

if __name__ == "__main__":
    run_ty_main(env_var="SYNC_CONFIG_PATH", default_config="sync_config.json", market="usc")
