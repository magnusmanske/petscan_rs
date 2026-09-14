#!/bin/zsh
# Opens the SSH tunnels PetScan needs for local development, then checks that
# each one answers a query.
#
# Every entry below is `local_port remote_host`, where `local_port` is the port
# the matching `port_mapping` key in config.json points at, and `remote_host`
# is the Wiki Replica service. The Commons links tables (categorylinks,
# pagelinks, templatelinks, langlinks, …) live on their own cluster since
# September 2026, reached by prefixing the host with `links.`, so Commons needs
# two tunnels:
# https://wikitech.wikimedia.org/wiki/News/2026_Commons_links_tables_database_split

LOGIN=magnus@login.toolforge.org

TUNNELS=(
	"3307 dewiki.web.db.svc.wikimedia.cloud"
	"3309 wikidatawiki.web.db.svc.wikimedia.cloud"
	"3305 commonswiki.web.db.svc.wikimedia.cloud"
	"3315 links.commonswiki.web.db.svc.wikimedia.cloud"
	"3310 enwiki.web.db.svc.wikimedia.cloud"
	"3308 tools.db.svc.wikimedia.cloud"
	"3317 termstore.wikidatawiki.analytics.db.svc.wikimedia.cloud"
)

# One probe per tunnel, touching a table only that host has, so a pass proves
# the tunnel reaches the right cluster and not just some MySQL. Kept to a
# single indexed row — a COUNT(*) over a replica table takes minutes.
declare -A PROBE
PROBE[3307]="SELECT page_id FROM dewiki_p.page LIMIT 1"
PROBE[3309]="SELECT page_id FROM wikidatawiki_p.page LIMIT 1"
PROBE[3305]="SELECT img_name FROM commonswiki_p.image LIMIT 1"
PROBE[3315]="SELECT lt_id FROM commonswiki_p.linktarget LIMIT 1"
PROBE[3310]="SELECT page_id FROM enwiki_p.page LIMIT 1"
PROBE[3308]="SELECT 1"
PROBE[3317]="SELECT wby_id FROM wikidatawiki_p.wbt_type LIMIT 1"

for tunnel in $TUNNELS; do
	port=${tunnel%% *}
	host=${tunnel#* }
	if nc -z 127.0.0.1 $port 2>/dev/null; then
		echo "port $port already in use, not tunnelling $host"
		continue
	fi
	echo "tunnelling $host -> 127.0.0.1:$port"
	ssh $LOGIN -L $port:$host:3306 -N -f
done

# Credentials: the same ones PetScan itself uses locally, i.e. the `user` /
# `password` from config.json (see DatabaseManager::credentials).
USER=$(grep -o '"user": *"[^"]*"' config.json | head -1 | cut -d'"' -f4)
PASS=$(grep -o '"password": *"[^"]*"' config.json | head -1 | cut -d'"' -f4)
if [[ -z $USER ]]; then
	echo "no \"user\" in config.json, cannot probe the tunnels" >&2
	exit 1
fi

echo
failed=0
for tunnel in $TUNNELS; do
	port=${tunnel%% *}
	host=${tunnel#* }
	out=$(mysql -h 127.0.0.1 -P $port -u "$USER" -p"$PASS" -N -B \
		--connect-timeout=10 -e "${PROBE[$port]}" 2>&1)
	if [[ $? -eq 0 ]]; then
		echo "ok    $port $host -> $out"
	else
		echo "FAIL  $port $host -> $out"
		failed=1
	fi
done

exit $failed
