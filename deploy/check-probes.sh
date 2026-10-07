#!/usr/bin/env bash
# Probe tripwire (ADR-211): render the Helm chart and check what the kubelet will ask of
# each workload.
#
# - The coordinator's liveness and startup probes do not point at /_health. That endpoint
#   answers for the whole cluster (every shard, the control plane, an admission slot), so a
#   liveness probe on it restarts a healthy coordinator because something else is down.
# - Every probe sets timeoutSeconds. The kubelet's default is one second.
# - The coordinator and the shards have a startup probe: neither serves its probe port
#   until it has assembled or opened what it serves.
# - The coordinator's liveness and startup probes carry their own
#   terminationGracePeriodSeconds, so a liveness kill does not wait out the pod's grace
#   period, which is sized for a rebalance drain.
# - All of that also holds for a release upgraded with `helm upgrade --reuse-values`, which
#   renders with the values the release was installed with. A release from before these
#   settings existed has no `probes` map, so the templates carry the defaults too, and the
#   two renders have to agree.
#
# Runs per-PR in the `helm chart` CI job. Requires: helm, awk.
set -euo pipefail

cd "$(dirname "$0")/.."
fail() { echo "FAIL: $*" >&2; exit 1; }
command -v helm >/dev/null || fail "missing tool: helm"

# One line per probe in a render: "<workload> <probe> path=<p> timeout=<n> grace=<n>",
# with "-" for what the probe does not set.
probes() {
  awk '
    function flush() {
      if (probe != "") print name, probe, "path=" path, "timeout=" timeout, "grace=" grace
      probe = ""
    }
    /^---/ { flush(); name = ""; next }
    /^  name: / { if (name == "") name = $2 }
    {
      indent = match($0, /[^ ]/) - 1
      if (probe != "" && indent <= at) flush()
    }
    /^ *(startup|liveness|readiness)Probe:/ {
      at = match($0, /[^ ]/) - 1
      probe = $1; sub(/Probe:$/, "", probe)
      path = "-"; timeout = "-"; grace = "-"
      next
    }
    # A key rendered with no value (a template that read a setting which is not there) is
    # as good as absent.
    probe != "" && $1 == "path:" && $2 != "" { path = $2 }
    probe != "" && $1 == "timeoutSeconds:" && $2 != "" { timeout = $2 }
    probe != "" && $1 == "terminationGracePeriodSeconds:" && $2 != "" { grace = $2 }
    END { flush() }
  '
}

CHART=deploy/helm/reverse-rusty

check() { # label, then helm --set arguments; renders $CHART
  local label=$1
  shift
  local rendered found coordinator shard
  rendered=$(helm template rr "$CHART" "$@" 2>&1) ||
    fail "$label: helm template failed: $(tail -1 <<<"$rendered")"
  found=$(probes <<<"$rendered")
  [[ -n "$found" ]] || fail "$label: no probes found in the render"
  coordinator=$(grep -- '-coordinator ' <<<"$found" || true)
  shard=$(grep -- '-shard ' <<<"$found" || true)

  while read -r name probe path timeout grace; do
    [[ "$timeout" != "timeout=-" ]] || fail "$label: $name $probe probe sets no timeoutSeconds"
  done <<<"$found"

  for probe in startup liveness readiness; do
    grep -q " $probe " <<<"$coordinator" || fail "$label: the coordinator has no $probe probe"
  done
  grep -q " startup " <<<"$shard" || fail "$label: the shards have no startup probe"

  for probe in startup liveness; do
    line=$(grep " $probe " <<<"$coordinator")
    case "$line" in
      *" path=/_health "* | *" path=/_health?"*)
        fail "$label: the coordinator's $probe probe points at /_health, which answers for the whole cluster" ;;
      *" path=- "*) fail "$label: the coordinator's $probe probe has no HTTP path" ;;
    esac
    [[ "$line" != *" grace=-" ]] ||
      fail "$label: the coordinator's $probe probe sets no terminationGracePeriodSeconds"
  done
  [[ "$(grep ' startup ' <<<"$coordinator" | awk '{print $3}')" == \
    "$(grep ' liveness ' <<<"$coordinator" | awk '{print $3}')" ]] ||
    fail "$label: the coordinator's startup and liveness probes ask different paths"
  echo "  ok: $label ($(wc -l <<<"$found" | tr -d ' ') probes)"
}

echo "==> probes (rendered Helm chart)"
check "default values"
check "resolve-only coordinator" --set coordinator.resolveOnly=true
check "no control plane" --set controlPlane.enabled=false
check "strict readiness" --set coordinator.probes.readiness.path=/_health

# The same chart with the values of a release installed before the probe settings existed.
with_defaults=$(helm template rr "$CHART" 2>/dev/null | probes)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cp -R "$CHART" "$work/chart"
awk '
  /^  probes:[ ]*$/ { skipping = 1; next }
  skipping && (/^    / || /^[ ]*$/) { next }
  { skipping = 0; print }
' "$CHART/values.yaml" >"$work/chart/values.yaml"
grep -q 'probes:' "$work/chart/values.yaml" && fail "could not strip the probe settings from a copy of values.yaml"
CHART="$work/chart"
check "values without probe settings (helm upgrade --reuse-values)"
[[ "$(helm template rr "$CHART" 2>/dev/null | probes)" == "$with_defaults" ]] ||
  fail "the probe defaults in the templates differ from the ones in values.yaml"
echo "  ok: the templates and values.yaml agree on the probe defaults"
echo "PASS: every probe has a timeout and none restarts the coordinator for its dependencies"
