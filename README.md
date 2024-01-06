# PetScan

PetScan is a powerful querying tool for Wikimedia. A query is prepared in the PetScan submission form.

Live: https://petscan.wmflabs.org/

Docs: https://meta.wikimedia.org/wiki/PetScan/en


## Development

### Prerequisites

* [Rust](https://www.rust-lang.org/)
* MySQL server
* [Toolforge account](https://wikitech.wikimedia.org/wiki/Help:Toolforge)


### Setup local MySQL database on port 3308

```sql
CREATE TABLE `query` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `querystring` longtext DEFAULT NULL,
  `created` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE `started_queries` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `querystring` longtext DEFAULT NULL,
  `created` varchar(100) DEFAULT NULL,
  `process_id` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
);
```


### Forward replicas

```bash
ssh login.toolforge.org -L 3306:XXX.analytics.db.svc.wikimedia.cloud:3306 -L 3309:wikidatawiki.analytics.db.svc.wikimedia.cloud:3306
```

XXX: wiki to be queried (e.g. commonswiki)

🔗 https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database


### Create config.json
```json
{
  "host": "127.0.0.1",
  "user": "<databaseusername>",
  "password": "<databasepassword>",
  "schema": "petscan",
  "http_port": 8000,
  "timeout": 30000,
  "restart-code": "",
  "mysql": [
    [
      "<replicausername>",
      "<replicapassword>"
    ]
  ]
}
```

The credentials to the database replicas can be found in `~/replica.my.cnf` of the Toolforge user account.


### Start server

```bash
cargo run
```
