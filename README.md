[![Codacy Badge](https://app.codacy.com/project/badge/Grade/65e4e917be864e4889450346470ddb3b)](https://app.codacy.com/gh/magnusmanske/petscan_rs/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)

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

One tunnel per replica host you want to query:

```bash
ssh login.toolforge.org -L 3306:XXX.web.db.svc.wikimedia.cloud:3306 -L 3309:wikidatawiki.web.db.svc.wikimedia.cloud:3306
```

XXX: wiki to be queried (e.g. commonswiki)

Some wikis keep part of their tables in an *extension database* on a separate
replica host, reached by prefixing the hostname. There are two of these:

- `links.commonswiki.web.db.svc.wikimedia.cloud` holds Commons' `categorylinks`,
  `pagelinks`, `templatelinks`, `langlinks`, `imagelinks`, `globalimagelinks`,
  `externallinks`, `iwlinks`, `linktarget`, `collation` and `existencelinks`
  ([split off in September 2026](https://wikitech.wikimedia.org/wiki/News/2026_Commons_links_tables_database_split));
  `page` and `redirect` are on both hosts.
- `termstore.wikidatawiki.analytics.db.svc.wikimedia.cloud` holds Wikidata's
  `wbt_*` term-store tables.

`connect_test_sql.sh` opens every tunnel `PetScan` needs and checks each one
with a query — start from that rather than the command above.

🔗 https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database


### Create config.json
Put the ports from the above `ssh` command into `port_mapping`, keyed by the
leading labels of the replica hostname:

```json
{
  "host": "127.0.0.1",
  "user": "<databaseusername>",
  "password": "<databasepassword>",
  "schema": "petscan",
  "http_port": 8000,
  "timeout": 30000,
  "restart-code": "",
  "port_mapping":{
    "<xxx>":3306,
    "wikidatawiki":3309,
    "links.commonswiki":3315,
    "termstore.wikidatawiki":3317
  },
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

### Run a query from command line

You can run a query from command line using the URL parameters. Output will be in the specified format, except HTML whcih will be automatically rewritten to JSON.
```bash
cargo run -- 'url_parameters'
```
