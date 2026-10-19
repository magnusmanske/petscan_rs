#!/bin/zsh
# Runs the live tests (the #[ignore]d ones, which query the replicas) through
# SSH tunnels that toolforge-tunnel opens for the duration of the run. The
# tunnels and credentials come from the [tunnels] and [client] sections of
# ./replica.my.cnf. Extra arguments go to the test binary, e.g. a test name.
#
# If every tunnel is already open (e.g. a toolforge-tunnel in another
# terminal), those are used and left alone.

cd "${0:A:h}"
export TOOL_DATA_DIR=.

# The local ports of the tunnels in replica.my.cnf
ports=($(sed -n '/^\[tunnels\]/,/^\[/s/^[^#;[][^=]*=[[:space:]]*\([0-9][0-9]*\).*/\1/p' replica.my.cnf))
if (( ! $#ports )); then
	echo "no [tunnels] in replica.my.cnf" >&2
	exit 1
fi

all_ports_open() {
	local port
	for port in $ports; do
		nc -z 127.0.0.1 $port 2>/dev/null || return 1
	done
}

if ! all_ports_open; then
	toolforge-tunnel &
	tunnel=$!
	# toolforge-tunnel closes its ssh on SIGINT (Ctrl-C), but leaves it
	# running, and the ports open, on SIGTERM.
	close_tunnels() {
		local ssh_pid=$(pgrep -P $tunnel)
		kill -INT $tunnel 2>/dev/null
		wait $tunnel 2>/dev/null
		[[ -n $ssh_pid ]] && kill $ssh_pid 2>/dev/null
	}
	trap close_tunnels EXIT
	trap 'exit 130' INT TERM

	for i in {1..30}; do
		all_ports_open && break
		sleep 1
	done
	if ! all_ports_open; then
		echo "toolforge-tunnel did not open all tunnels within 30 s" >&2
		exit 1
	fi
fi

cargo test --lib -- --ignored "$@"
