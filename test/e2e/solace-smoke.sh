#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NAMESPACE="${NAMESPACE:-default}"
SETUP_ONLY="${1:-}"

log() {
  echo "[solace-smoke] $*"
}

wait_for_deployment_label() {
  local label="$1"
  local deployment=""
  local start_time
  start_time="$(date +%s)"

  while [[ -z "${deployment}" ]]; do
    deployment="$(kubectl -n "${NAMESPACE}" get deploy -l "${label}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    if [[ -n "${deployment}" ]]; then
      break
    fi
    if (( "$(date +%s)" - start_time > 180 )); then
      echo "timed out waiting for deployment with label ${label}" >&2
      return 1
    fi
    sleep 2
  done

  kubectl -n "${NAMESPACE}" rollout status "deploy/${deployment}" --timeout=180s >/dev/null
  echo "${deployment}"
}

wait_for_log_match() {
  local deployment="$1"
  local pattern="$2"
  local start_time
  start_time="$(date +%s)"

  while true; do
    if kubectl -n "${NAMESPACE}" logs "deploy/${deployment}" --since=5m 2>/dev/null | grep -Fq "${pattern}"; then
      return 0
    fi
    if (( "$(date +%s)" - start_time > 180 )); then
      echo "timed out waiting for log pattern '${pattern}' in ${deployment}" >&2
      return 1
    fi
    sleep 2
  done
}

apply_examples() {
  log "applying Solace broker, RBAC, and example resources in ${NAMESPACE}"
  kubectl -n "${NAMESPACE}" apply -f "${ROOT_DIR}/examples/rbac/eventsource-lease-rbac.yaml" >/dev/null
  kubectl -n "${NAMESPACE}" apply -f "${ROOT_DIR}/examples/eventbus/solace-broker.yaml" >/dev/null
  kubectl -n "${NAMESPACE}" rollout status statefulset/solace --timeout=300s >/dev/null
  kubectl -n "${NAMESPACE}" apply -f "${ROOT_DIR}/examples/eventbus/solace.yaml" >/dev/null
  kubectl -n "${NAMESPACE}" apply -f "${ROOT_DIR}/examples/event-sources/solace.yaml" >/dev/null
  kubectl -n "${NAMESPACE}" apply -f "${ROOT_DIR}/examples/event-sources/webhook.yaml" >/dev/null
  kubectl -n "${NAMESPACE}" apply -f "${ROOT_DIR}/examples/sensors/solace.yaml" >/dev/null
  kubectl -n "${NAMESPACE}" apply -f "${ROOT_DIR}/examples/sensors/solace-trigger.yaml" >/dev/null
}

apply_examples

solace_eventsource_deploy="$(wait_for_deployment_label 'eventsource-name=solace')"
webhook_eventsource_deploy="$(wait_for_deployment_label 'eventsource-name=webhook')"
solace_sensor_deploy="$(wait_for_deployment_label 'sensor-name=solace')"
solace_trigger_deploy="$(wait_for_deployment_label 'sensor-name=solace-trigger')"

log "ready deployments: ${solace_eventsource_deploy}, ${webhook_eventsource_deploy}, ${solace_sensor_deploy}, ${solace_trigger_deploy}"

if [[ "${SETUP_ONLY}" == "--setup-only" ]]; then
  log "setup complete"
  exit 0
fi

mqtt_token="post-scratch-$(date +%s)"
webhook_token="webhook-solace-$(date +%s)"
sub_pod="solace-sub-$(date +%s)"
pub_pod="webhook-pub-$(date +%s)"

log "publishing MQTT test event on events/argo/testing"
kubectl -n "${NAMESPACE}" run "solace-pub-$(date +%s)" \
  --image=eclipse-mosquitto:2 --restart=Never --rm -i --quiet \
  --command -- sh -lc "mosquitto_pub -h solace.default -p 1883 -u admin -P admin -t events/argo/testing -m '{\"hello\":\"world\",\"test\":\"${mqtt_token}\"}'" >/dev/null

wait_for_log_match "${solace_sensor_deploy}" "${mqtt_token}"

log "publishing webhook event and waiting for Solace trigger output on events/solace/outbound"
kubectl -n "${NAMESPACE}" delete pod "${sub_pod}" --ignore-not-found >/dev/null
kubectl -n "${NAMESPACE}" run "${sub_pod}" \
  --image=eclipse-mosquitto:2 --restart=Never \
  --command -- sh -lc "mosquitto_sub -h solace.default -p 1883 -u admin -P admin -t events/solace/outbound -C 1 -W 25 -v" >/dev/null
kubectl -n "${NAMESPACE}" wait --for=condition=Ready "pod/${sub_pod}" --timeout=60s >/dev/null

kubectl -n "${NAMESPACE}" run "${pub_pod}" \
  --image=curlimages/curl:8.12.1 --restart=Never \
  --command -- sh -lc "curl -sS -X POST -H 'Content-Type: application/json' --data '{\"hello\":\"solace-trigger\",\"source\":\"webhook\",\"test\":\"${webhook_token}\"}' http://webhook-eventsource-svc.${NAMESPACE}:12000/example" >/dev/null
kubectl -n "${NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/${pub_pod}" --timeout=60s >/dev/null

kubectl -n "${NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/${sub_pod}" --timeout=60s >/dev/null
sub_output="$(kubectl -n "${NAMESPACE}" logs "pod/${sub_pod}")"
kubectl -n "${NAMESPACE}" delete pod "${sub_pod}" --ignore-not-found >/dev/null
kubectl -n "${NAMESPACE}" delete pod "${pub_pod}" --ignore-not-found >/dev/null

if [[ "${sub_output}" != *"${webhook_token}"* ]]; then
  echo "expected webhook token ${webhook_token} in Solace outbound message, got: ${sub_output}" >&2
  exit 1
fi

wait_for_log_match "${solace_trigger_deploy}" 'successfully published message to solace'

log "Solace smoke test passed"
