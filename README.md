# tiflash-replica-table-data-balancer

## Table balancer
A tool helps to balance the table data of TiFlash replicas between multiple TiFlash instances.

### Usage

```bash
./balancer http --table <table_id> [--pd-host <host>] [--pd-port <port>] [--ssl-ca <ca>] [--ssl-cert <cert>] [--ssl-key <key>]
```

or

```bash
./balancer ctl --table <table_id> --ctl-path <path_of_pd_ctl>
```

or

```bash
./balancer local --table <table_id> --regions <file_1> <file_2> --stores <store_file>
```


### How to get table_id

Connect to TiDB and run the following SQL:

```sql
SELECT TIDB_TABLE_ID FROM information_schema.tables WHERE table_schema = '<database>' AND table_name = '<table>';
+---------------+
| TIDB_TABLE_ID |
+---------------+
|    <table_id> |
+---------------+
```

## Dead region balancer

Some TiFlash stores could fail, result in several regions having no living TiFlash peers. This tool helps to create peer for those dead regions on some other living TiFlash nodes.

### Usage
Only supports PD's HTTP api.
```bash
./balancer dead_region --stores <stores_list_split_by_comma> [--pd-host <host>] [--pd-port <port>] [--ssl-ca <ca>] [--ssl-cert <cert>] [--ssl-key <key>]
```

Specifying `--offline` to also delete `<stores_list_split_by_comma>` from PD, which means they will be set at state `offline`.