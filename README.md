# tiflash-replica-table-data-balancer

A tool helps to balance the table data of TiFlash replicas between multiple TiFlash instances.

## Usage

```bash
./balancer --table <table_id> [--pd-host <host>] [--pd-port <port>] [--ssl-ca <ca>] [--ssl-cert <cert>] [--ssl-key <key>]
```

## How to get table_id

Connect to TiDB and run the following SQL:

```sql
SELECT TIDB_TABLE_ID FROM information_schema.tables WHERE table_schema = '<database>' AND table_name = '<table>';
+---------------+
| TIDB_TABLE_ID |
+---------------+
|    <table_id> |
+---------------+
```
