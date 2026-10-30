[![Codacy Badge](https://app.codacy.com/project/badge/Grade/65e4e917be864e4889450346470ddb3b)](https://app.codacy.com/gh/magnusmanske/petscan_rs/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)

# PetScan

PetScan is a powerful querying tool for Wikimedia. A query is prepared in the PetScan submission form.

Live: https://petscan.toolforge.org/ (see "Deploy on Toolforge" below)

Docs: https://meta.wikimedia.org/wiki/PetScan/en


## Development

### Prerequisites

* [Rust](https://www.rust-lang.org/)
* MySQL server
* [Toolforge account](https://wikitech.wikimedia.org/wiki/Help:Toolforge)

### Setup local MySQL database on port 3308

Or tunnel to the tool's database on ToolsDB instead (`connect_test_sql.sh` does).

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

PetScan reaches each wiki through its section (`s1` to `s8`), so it needs one
tunnel per section, including `s7`, whose `meta_p` tells which wiki is on which
section:

```bash
ssh login.toolforge.org -L 3401:s1.web.db.svc.wikimedia.cloud:3306 -L 3407:s7.web.db.svc.wikimedia.cloud:3306 ...
```

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


### Create replica.my.cnf

Database servers and credentials come from `replica.my.cnf`, read by the
[`toolforge`](https://crates.io/crates/toolforge) crate from `$TOOL_DATA_DIR`
or the home directory (it does not look in the working directory). Start from
the `~/replica.my.cnf` of the Toolforge tool account, and for local development
add `local='true'` and the ports of your SSH tunnels, keyed by the leading
labels of the replica hostname (`toolsdb` for the tool database):

```ini
[client]
user='<databaseusername>'
password='<databasepassword>'
local='true'

[tunnels]
s1 = 3401
s2 = 3402
s3 = 3403
s4 = 3404
s5 = 3405
s6 = 3406
s7 = 3407
s8 = 3408
links.commonswiki = 3315
termstore.wikidatawiki = 3317
toolsdb = 3308
```

Sections without a tunnel entry use port 3306 + N (e.g. 3307 for `s1`), other
replicas 3306. A local MySQL server standing in
for the tool database must accept the same user and password.

`replica.my.cnf` is gitignored, so it can live in the repository; then run
PetScan with `TOOL_DATA_DIR=.` (as `run.sh` does in production).

### Create config.json

```json
{
  "schema": "petscan",
  "http_port": 8000,
  "timeout": 30000,
  "restart-code": ""
}
```

`config.json.template` lists the remaining settings.


### Start server

```bash
TOOL_DATA_DIR=. cargo run
```

### Run a query from command line

You can run a query from command line using the URL parameters. Output will be in the specified format, except HTML whcih will be automatically rewritten to JSON.
```bash
TOOL_DATA_DIR=. cargo run -- 'url_parameters'
```


## Deploy on Toolforge

PetScan can also run as the `petscan` tool's web service, built by the
[Toolforge Build Service](https://wikitech.wikimedia.org/wiki/Help:Toolforge/Building_container_images)
from the `Procfile`. As the tool (`become petscan`):

1. Put a `config.json` into the tool's home directory, `/data/project/petscan/`
   (the `Procfile` points `PETSCAN_CONFIG` there), with `"http_port": 8000`,
   the port Toolforge expects a web service on. Memory is tighter than on the
   VPS, so consider a lower `max_result_entries` (about 2 GB per million).
2. For now PetScan keeps using the VPS's database account (tool account
   `s51156`, whose ToolsDB holds `s51156__petscan`), not the `petscan` tool's
   own. Copy its credentials into e.g. `/data/project/petscan/s51156.my.cnf`,
   without `local` or `[tunnels]`, and set `"db_credentials_file"` in
   `config.json` to that path. Without it, PetScan would use the credentials
   Toolforge provides the `petscan` tool. `chmod 600` both files.
3. Push the code to GitHub, then run `./build.sh` to build the image and
   `./restart.sh` to (re)start the web service. Logs:
   `toolforge webservice buildservice logs -f`. The `Procfile` sets
   `RUST_BACKTRACE=1`, so a panic arrives there with a backtrace; after an
   abrupt exit (for example the container's memory limit), the next start
   logs the queries the killed process was running (see
   `src/previous_run.rs`).

While both deployments use the same database account, they share its limit
of concurrent connections per replica.
