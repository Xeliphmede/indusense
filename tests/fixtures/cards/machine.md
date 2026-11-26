# Carte d'identité : machine.sql.txt

- Source: /home/thopuetai/Formation IA/indusense/datas/machine.sql.txt
- Filesize: 22.41 Ko
- ProcessBeginAt: 15:38:20
- ProcessEndAt: 15:38:20
- Charset: UTF-8
- SQL Dialect: postgres
- Statements Number: 10
- Tables Number: 2

## Table `machine`

- Rows Number: 15
- Cols Number: 11

### Column list

| Colname | SQL Type | Inferred Type |
| --- | --- | --- |
| machine_code | VARCHAR(16) | IDENTITY |
| commissioning_date | DATE | DATE |
| max_daily_capacity | INT | INTEGER |
| max_hourly_capacity_pieces | INT | INTEGER |
| model | VARCHAR(32) | CATEGORY |
| production_line | VARCHAR(16) | CATEGORY |
| location | VARCHAR(16) | CATEGORY |
| criticality | VARCHAR(8) | CATEGORY |
| is_active | BOOLEAN | - |
| created_at | TIMESTAMPTZ | - |
| updated_at | TIMESTAMPTZ | - |

### Numeric and date columns

| Colname | Min value | Max value | Mean value | Median value |
| --- | --- | --- | --- | --- |
| commissioning_date | 2019-07-23 | 2025-05-25 | - | - |
| max_daily_capacity | 750 | 1428 | 1054.867 | 1027.000 |
| max_hourly_capacity_pieces | 47 | 89 | 65.867 | 64.000 |

## Table `maintenance`

- Rows Number: 115
- Cols Number: 11

### Column list

| Colname | SQL Type | Inferred Type |
| --- | --- | --- |
| maintenance_id | INT | IDENTITY |
| machine_code | VARCHAR(16) | IDENTITY |
| maintenance_at | TIMESTAMPTZ | DATETIME (UTC) |
| maintenance_type | VARCHAR(16) | CATEGORY |
| action_type | VARCHAR(32) | CATEGORY |
| component | VARCHAR(64) | CATEGORY |
| description | TEXT | TEXT |
| related_incident_id | VARCHAR(16) | IDENTITY |
| duration_hours | DECIMAL(6, 2) | FLOAT |
| created_at | TIMESTAMPTZ | - |
| updated_at | TIMESTAMPTZ | - |

### Numeric and date columns

| Colname | Min value | Max value | Mean value | Median value |
| --- | --- | --- | --- | --- |
| maintenance_at | 2025-06-15 06:00:00 UTC | 2026-05-30 22:00:00 UTC | - | - |
| duration_hours | 1.000 | 8.000 | 2.943 | 2.640 |
