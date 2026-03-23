from export_redshift_table_common import export_table
import sys

if __name__ == "__main__":
    sys.exit(export_table("deposit", "csv"))
