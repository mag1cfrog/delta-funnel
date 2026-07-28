#!/bin/sh

# Test double for the operation-capture lifecycle. The immediate parent-memory
# probe makes any authorize-after-exec race deterministic.
if [ "${1:-}" = "--version" ]; then
    exit 0
fi

trace=
while [ "$#" -gt 0 ]; do
    if [ "$1" = "--out" ]; then
        shift
        trace=${1:-}
        break
    fi
    shift
done

if [ -r /proc/sys/kernel/yama/ptrace_scope ] &&
    [ "$(cat /proc/sys/kernel/yama/ptrace_scope)" = 1 ]; then
    : <"/proc/$PPID/mem" || exit 77
fi

if [ -n "$trace" ]; then
    printf '%s\n' "$$" >"${trace%.*}.pid"
fi

sleeper=
cleanup() {
    if [ -n "$sleeper" ]; then
        kill "$sleeper" 2>/dev/null || true
        wait "$sleeper" 2>/dev/null || true
    fi
    exit 0
}
trap cleanup TERM INT
# Publish readiness only after cleanup can identify the sleeper.
sleep 2147483647 &
sleeper=$!
printf '\000'
wait "$sleeper"
