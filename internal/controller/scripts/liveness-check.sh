#!/bin/sh
#
# Liveness check of a Valkey node (POSIX sh, works with bash, dash & busybox ash).
#
# Usage: liveness-check.sh [timeout] [port]
#        Default timeout is 4s, and default Valkey port is 6379.
set -e

timeout=${1:-"4"}
port=${2:-"6379"}

# Run a command with a DURATION (seconds) timeout. A watchdog sends SIGTERM on
# timeout, then SIGKILL a second later. Returns the command's exit code, or 124
# on timeout. It starts one sleep however long the command takes, so a slow host
# does not make each probe start more processes.
timeout_cmd() {
    duration=$1; shift
    "$@" &
    cmdpid=$!
    # Every process here is waited for by its parent: valkey-server runs as
    # PID 1 and does not reap orphans, so each one left would stay a zombie.
    # The watchdog must not hold the caller's stdout, or $(...) waits for it.
    (
        sleeper=
        trap 'kill "$sleeper" 2>/dev/null; wait; exit 0' TERM
        sleep "$duration" & sleeper=$!
        wait "$sleeper"
        kill -TERM "$cmdpid" 2>/dev/null
        sleep 1 & sleeper=$!
        wait "$sleeper"
        kill -KILL "$cmdpid" 2>/dev/null
    ) >/dev/null 2>&1 &
    watchdog=$!
    rc=0
    wait "$cmdpid" || rc=$?
    kill "$watchdog" 2>/dev/null || true
    wait "$watchdog" 2>/dev/null || true
    # Killed by the watchdog's SIGTERM (143) or SIGKILL (137).
    if [ "$rc" -eq 143 ] || [ "$rc" -eq 137 ]; then
        return 124
    fi
    return "$rc"
}

# Build TLS args from environment variables if set
tls_args=""
if [ -n "${VALKEY_TLS_ARGS:-}" ]; then
    tls_args="$VALKEY_TLS_ARGS"
fi

# Authenticate as the custom user when user and password are set.
auth_args=""
if [ -n "${VALKEY_USER:-}" ] && [ -n "${VALKEYCLI_AUTH:-}" ]; then
    auth_args="--user $VALKEY_USER"
fi

# Perform check
response=$(
    timeout_cmd $timeout \
    valkey-cli -h localhost -p $port $tls_args $auth_args PING)

nl='
'
firstLine=${response%%"$nl"*}
responseFirstWord=${firstLine%% *}
if [ "$response" != "PONG" ] && [ "$responseFirstWord" != "LOADING" ] && [ "$responseFirstWord" != "MASTERDOWN" ]; then
    echo "$response" >&2
    exit 1
fi
