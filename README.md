# Redshift to Supabase Migration Tool

Tool untuk migrate data dari Amazon Redshift ke Supabase (PostgreSQL) menggunakan Python.

## Features

✅ Transfer data per table atau semua tables sekaligus  
✅ Automatic schema creation di Supabase  
✅ Batch processing untuk handle data yang besar  
✅ Progress bar untuk tracking progress  
✅ Data type conversion dari Redshift ke PostgreSQL  
✅ Error handling dan migration summary  

## Prerequisites

- Python 3.8+
- Access ke Redshift database
- Supabase project dengan database credentials

## Installation

1. Clone atau download project ini

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Copy `.env.example` ke `.env` dan isi dengan credentials:
```bash
cp .env.example .env
```

4. Edit file `.env` dengan credentials kamu:
```env
# Redshift Configuration
REDSHIFT_HOST=your-redshift-cluster.region.redshift.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DATABASE=dev
REDSHIFT_USER=your_username
REDSHIFT_PASSWORD=your_password

# Supabase Configuration
SUPABASE_HOST=db.your-project.supabase.co
SUPABASE_PORT=5432
SUPABASE_DATABASE=postgres
SUPABASE_USER=postgres
SUPABASE_PASSWORD=your_supabase_password
```

## Usage

### Cara 1: Interactive Mode

Jalankan script dan ikuti petunjuk:

```bash
python migrate.py
```

Kamu akan diminta pilih:
1. **Migrate specific table(s)** - Pilih table tertentu yang mau di-migrate
2. **Migrate all tables** - Migrate semua tables sekaligus
3. **List all tables** - Lihat daftar tables dan jumlah rows

### Cara 2: Programmatic Mode

Import dan gunakan class `RedshiftToSupabase`:

```python
from migrate import RedshiftToSupabase

migrator = RedshiftToSupabase()

# Connect
migrator.connect_redshift()
migrator.connect_supabase()

# Migrate single table
migrator.migrate_table('users', batch_size=1000)

# Migrate all tables
migrator.migrate_all_tables(batch_size=1000)

# Close connections
migrator.close_connections()
```

## Configuration

### Batch Size

Default batch size adalah 1000 rows per insert. Kamu bisa adjust sesuai kebutuhan:
- **Smaller batch** (500-1000): Lebih safe, tapi lebih lambat
- **Larger batch** (5000-10000): Lebih cepat, tapi butuh memory lebih besar

### Schema Names

Default menggunakan schema `public`. Kalau mau custom schema:

```python
migrator.migrate_table(
    'table_name', 
    source_schema='my_schema',  # Redshift schema
    target_schema='public',      # Supabase schema
    batch_size=1000
)
```

## Data Type Mapping

Tool ini otomatis convert Redshift data types ke PostgreSQL:

| Redshift | PostgreSQL |
|----------|------------|
| INTEGER | INTEGER |
| BIGINT | BIGINT |
| SMALLINT | SMALLINT |
| VARCHAR(n) | VARCHAR(n) |
| CHAR(n) | CHAR(n) |
| TEXT | TEXT |
| NUMERIC | NUMERIC |
| REAL | REAL |
| DOUBLE PRECISION | DOUBLE PRECISION |
| BOOLEAN | BOOLEAN |
| DATE | DATE |
| TIMESTAMP | TIMESTAMP |
| TIMESTAMPTZ | TIMESTAMPTZ |

## Troubleshooting

### Connection Error

Pastikan:
- Credentials di `.env` sudah benar
- Redshift cluster bisa diakses (check security group)
- Supabase database bisa diakses (check connection pooler settings)

### Memory Error

Kalau data terlalu besar:
- Kurangi `batch_size`
- Migrate table satu per satu instead of all at once

### Data Type Error

Kalau ada data type yang belum disupport, tambahkan di function `convert_datatype()` di `migrate.py`

## Notes

⚠️ **IMPORTANT:**
- Tool ini akan create table baru di Supabase. Kalau table sudah ada, data akan di-append (tidak di-replace)
- Pastikan backup data sebelum migration
- Test dulu dengan sample table sebelum migrate semua tables

## License

MIT License

## Support

Kalau ada issues atau questions, create issue di repository ini.

