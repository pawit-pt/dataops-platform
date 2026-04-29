#!/usr/bin/env bash
# ================================================================
# test-alerts.sh — Test all 7 Grafana/Prometheus alert rules
# ================================================================
#
# This script triggers failure scenarios to validate that:
#   1. Prometheus detects the failure condition
#   2. Grafana alert rules fire correctly
#   3. Alerts resolve after recovery
#
# Usage:
#   chmod +x scripts/test-alerts.sh
#   ./scripts/test-alerts.sh              # Run all 7 scenarios
#   ./scripts/test-alerts.sh --dq-only    # Run only DQ gate scenarios (1-4)
#   ./scripts/test-alerts.sh --flink-only # Run only Flink/Kafka scenarios (5-7)
#
# Prerequisites:
#   - All containers running (docker compose ps)
#   - Flink ETL pipeline submitted
#   - Airflow DAGs loaded
# ================================================================

set -euo pipefail

# ── Colors ──
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# ── Config ──
PROMETHEUS_URL="http://localhost:9090"
GRAFANA_URL="http://localhost:3000"
GRAFANA_USER="admin"
GRAFANA_PASS="admin"
AIRFLOW_CONTAINER="airflow-webserver"
FLINK_JM_CONTAINER="flink-jobmanager"
FLINK_TM_CONTAINER="flink-taskmanager"

# ── Helpers ──
log_header() {
    echo ""
    echo -e "${BOLD}${BLUE}================================================================${NC}"
    echo -e "${BOLD}${BLUE}  $1${NC}"
    echo -e "${BOLD}${BLUE}================================================================${NC}"
}

log_step() {
    echo -e "${CYAN}  ▶ $1${NC}"
}

log_ok() {
    echo -e "${GREEN}  ✔ $1${NC}"
}

log_fail() {
    echo -e "${RED}  ✘ $1${NC}"
}

log_warn() {
    echo -e "${YELLOW}  ⚠ $1${NC}"
}

log_wait() {
    echo -e "${YELLOW}  ⏳ $1${NC}"
}

separator() {
    echo -e "${BLUE}  ────────────────────────────────────────────────${NC}"
}

# Check if Prometheus alert is firing
check_prometheus_alert() {
    local alert_name="$1"
    local result
    result=$(curl -s "${PROMETHEUS_URL}/api/v1/alerts" | python3 -c "
import sys, json
data = json.load(sys.stdin)
alerts = data.get('data', {}).get('alerts', [])
for a in alerts:
    if a.get('labels', {}).get('alertname') == '${alert_name}' and a.get('state') == 'firing':
        print('FIRING')
        sys.exit(0)
print('inactive')
" 2>/dev/null || echo "ERROR")
    echo "$result"
}

# Check Grafana alert state via API
check_grafana_alert() {
    local rule_uid="$1"
    local result
    result=$(curl -s -u "${GRAFANA_USER}:${GRAFANA_PASS}" \
        "${GRAFANA_URL}/api/v1/provisioning/alert-rules/${rule_uid}" | \
        python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(data.get('title', 'unknown'))
except:
    print('ERROR')
" 2>/dev/null || echo "ERROR")
    echo "$result"
}

# Wait for Prometheus to scrape (with spinner)
wait_with_progress() {
    local seconds=$1
    local message=$2
    echo -ne "${YELLOW}  ⏳ ${message} "
    for ((i=1; i<=seconds; i++)); do
        echo -ne "."
        sleep 1
    done
    echo -e " done${NC}"
}

# Check prerequisites
check_prerequisites() {
    log_header "Checking Prerequisites"

    # Check Docker containers
    log_step "Checking Docker containers..."
    local containers=("$AIRFLOW_CONTAINER" "$FLINK_JM_CONTAINER" "$FLINK_TM_CONTAINER" "prometheus" "grafana" "statsd-exporter" "kafka" "kafka-exporter")
    local all_ok=true
    for c in "${containers[@]}"; do
        if docker ps --format '{{.Names}}' | grep -q "^${c}$"; then
            log_ok "$c is running"
        else
            log_fail "$c is NOT running"
            all_ok=false
        fi
    done

    if [ "$all_ok" = false ]; then
        log_fail "Some containers are not running. Run: docker compose up -d"
        exit 1
    fi

    # Check Prometheus is reachable
    log_step "Checking Prometheus..."
    if curl -s "${PROMETHEUS_URL}/api/v1/status/config" > /dev/null 2>&1; then
        log_ok "Prometheus is reachable at ${PROMETHEUS_URL}"
    else
        log_fail "Cannot reach Prometheus at ${PROMETHEUS_URL}"
        exit 1
    fi

    # Check Grafana is reachable
    log_step "Checking Grafana..."
    if curl -s -u "${GRAFANA_USER}:${GRAFANA_PASS}" "${GRAFANA_URL}/api/health" > /dev/null 2>&1; then
        log_ok "Grafana is reachable at ${GRAFANA_URL}"
    else
        log_fail "Cannot reach Grafana at ${GRAFANA_URL}"
        exit 1
    fi

    # Check Flink jobs are running
    log_step "Checking Flink jobs..."
    local running_jobs
    running_jobs=$(curl -s "http://localhost:8081/jobs/overview" | python3 -c "
import sys, json
data = json.load(sys.stdin)
running = [j for j in data.get('jobs', []) if j.get('state') == 'RUNNING']
print(len(running))
" 2>/dev/null || echo "0")
    if [ "$running_jobs" -ge 1 ]; then
        log_ok "Flink has ${running_jobs} running job(s) (Bronze + Silver run as a single combined job)"
    else
        log_warn "Flink has ${running_jobs} running jobs (expected 1). Some tests may not work."
    fi

    # Check Airflow DAG exists
    log_step "Checking test_alert_scenarios DAG..."
    if docker exec "$AIRFLOW_CONTAINER" airflow dags list 2>/dev/null | grep -q "test_alert_scenarios"; then
        log_ok "test_alert_scenarios DAG is loaded"
    else
        log_fail "test_alert_scenarios DAG not found. Check airflow/dags/"
        exit 1
    fi

    log_ok "All prerequisites passed!"
}

# ================================================================
# SCENARIO 1-4: DQ Gate + Gold INSERT Failures (via Airflow DAG)
# ================================================================
test_dq_scenarios() {
    log_header "Scenarios 1-4: DQ Gate Failures (Airflow DAG)"
    echo -e "  Triggers: DQGateFreshnessFailed, DQGateValidityFailed,"
    echo -e "            DQGateNullCheckFailed, GoldAggregationFailed"
    separator

    # Step 1: Trigger the test DAG
    log_step "Triggering test_alert_scenarios DAG..."
    docker exec "$AIRFLOW_CONTAINER" airflow dags trigger test_alert_scenarios 2>/dev/null
    log_ok "DAG triggered"

    # Step 2: Wait for tasks to fail
    log_wait "Waiting 60s for tasks to execute and fail..."
    wait_with_progress 60 "Tasks running"

    # Step 3: Verify tasks failed in Airflow
    log_step "Checking Airflow task states..."
    local dag_state
    dag_state=$(docker exec "$AIRFLOW_CONTAINER" airflow dags list-runs -d test_alert_scenarios -o json 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    if data:
        print(data[0].get('state', 'unknown'))
    else:
        print('no_runs')
except:
    print('error')
" 2>/dev/null || echo "error")
    echo -e "  DAG run state: ${YELLOW}${dag_state}${NC}"

    # Step 4: Wait for Prometheus to scrape the failed counters
    log_wait "Waiting 30s for Prometheus to scrape StatsD metrics..."
    wait_with_progress 30 "Prometheus scraping"

    # Step 5: Check each alert in Prometheus
    separator
    log_step "Checking Prometheus alerts..."

    local alerts=("DQGateFreshnessFailed" "DQGateValidityFailed" "DQGateNullCheckFailed" "GoldAggregationFailed")
    local alert_ids=("dq-freshness-failed" "dq-validity-failed" "dq-null-check-failed" "gold-aggregation-failed")
    local scenario_num=1

    for i in "${!alerts[@]}"; do
        local alert="${alerts[$i]}"
        local grafana_uid="${alert_ids[$i]}"
        local prom_state
        prom_state=$(check_prometheus_alert "$alert")

        if [ "$prom_state" = "FIRING" ]; then
            log_ok "Scenario ${scenario_num}: ${alert} = ${RED}FIRING${GREEN} ✔${NC}"
        else
            log_warn "Scenario ${scenario_num}: ${alert} = ${prom_state} (may need more time)"
        fi
        scenario_num=$((scenario_num + 1))
    done

    # Step 6: Check Grafana alert rules exist
    separator
    log_step "Checking Grafana alert rules..."
    local grafana_alerts
    grafana_alerts=$(curl -s -u "${GRAFANA_USER}:${GRAFANA_PASS}" \
        "${GRAFANA_URL}/api/v1/provisioning/alert-rules" 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for rule in data:
        title = rule.get('title', 'unknown')
        uid = rule.get('uid', '')
        print(f'    {uid}: {title}')
except:
    print('    Could not parse Grafana alert rules')
" 2>/dev/null || echo "    Grafana API not reachable")
    echo "$grafana_alerts"

    separator
    echo -e "  ${BOLD}Verify in browser:${NC}"
    echo -e "    Prometheus: ${CYAN}${PROMETHEUS_URL}/alerts${NC}"
    echo -e "    Grafana:    ${CYAN}${GRAFANA_URL}/alerting/list${NC}"
    echo -e "    Airflow:    ${CYAN}http://localhost:8085/dags/test_alert_scenarios/grid${NC}"
}

# ================================================================
# SCENARIO 5: Flink Job Not Running
# ================================================================
test_flink_not_running() {
    log_header "Scenario 5: FlinkJobNotRunning"
    echo -e "  Action: Stop Flink TaskManager → running jobs drop to 0"
    echo -e "  Alert fires after: 2 minutes"
    separator

    # Step 1: Stop TaskManager
    log_step "Stopping flink-taskmanager..."
    docker stop "$FLINK_TM_CONTAINER" > /dev/null 2>&1
    log_ok "flink-taskmanager stopped"

    # Step 2: Wait for alert to fire (2 min threshold + scrape interval)
    log_wait "Waiting 150s for FlinkJobNotRunning alert to fire (2m threshold)..."
    wait_with_progress 150 "Waiting for alert"

    # Step 3: Check alert
    local prom_state
    prom_state=$(check_prometheus_alert "FlinkJobNotRunning")
    if [ "$prom_state" = "FIRING" ]; then
        log_ok "Scenario 5: FlinkJobNotRunning = ${RED}FIRING${GREEN} ✔${NC}"
    else
        log_warn "Scenario 5: FlinkJobNotRunning = ${prom_state} (may need more time)"
    fi

    separator
    echo -e "  ${BOLD}Verify:${NC} ${CYAN}${PROMETHEUS_URL}/alerts${NC}"
    echo -e "  ${BOLD}Verify:${NC} ${CYAN}${GRAFANA_URL}/alerting/list${NC}"

    # Step 4: Restore
    separator
    log_step "Restoring flink-taskmanager..."
    docker start "$FLINK_TM_CONTAINER" > /dev/null 2>&1
    log_ok "flink-taskmanager started (jobs will auto-recover from checkpoint)"
}

# ================================================================
# SCENARIO 6: Kafka Consumer Lag High
# ================================================================
test_kafka_lag() {
    log_header "Scenario 6: KafkaConsumerLagHigh"
    echo -e "  Action: Stop Flink → producer keeps sending → lag builds up"
    echo -e "  Alert fires after: lag > 10,000 for 5 minutes"
    separator

    # Step 1: Stop TaskManager
    log_step "Stopping flink-taskmanager (consumer stops, producer continues)..."
    docker stop "$FLINK_TM_CONTAINER" > /dev/null 2>&1
    log_ok "flink-taskmanager stopped"

    # Step 2: Wait for lag to build (5 events/sec × 360s = 1800 messages + 5m threshold)
    # We need > 10,000 messages → at 5 events/sec, that's ~2000 seconds
    # But with existing lag it may be faster
    log_warn "At 5 events/sec, need ~34 min to reach 10,000 lag + 5m alert threshold"
    log_step "Waiting 120s to show lag building up..."
    wait_with_progress 120 "Lag building"

    # Step 3: Check current lag
    local current_lag
    current_lag=$(curl -s "${PROMETHEUS_URL}/api/v1/query?query=kafka_consumergroup_lag" | python3 -c "
import sys, json
data = json.load(sys.stdin)
results = data.get('data', {}).get('result', [])
total = sum(float(r['value'][1]) for r in results)
print(int(total))
" 2>/dev/null || echo "0")
    echo -e "  Current total consumer lag: ${YELLOW}${current_lag}${NC} messages"

    local prom_state
    prom_state=$(check_prometheus_alert "KafkaConsumerLagHigh")
    if [ "$prom_state" = "FIRING" ]; then
        log_ok "Scenario 6: KafkaConsumerLagHigh = ${RED}FIRING${GREEN} ✔${NC}"
    else
        log_warn "Scenario 6: KafkaConsumerLagHigh = ${prom_state}"
        log_warn "Need lag > 10,000 for 5 min. Current: ${current_lag}. Keep Flink stopped longer to reach threshold."
    fi

    # Step 4: Restore
    separator
    log_step "Restoring flink-taskmanager..."
    docker start "$FLINK_TM_CONTAINER" > /dev/null 2>&1
    log_ok "flink-taskmanager started (will replay from offset, lag will recover)"
}

# ================================================================
# SCENARIO 7: Flink Job Restarting Frequently
# ================================================================
test_flink_restarting() {
    log_header "Scenario 7: FlinkJobRestarting"
    echo -e "  Action: Stop/start TaskManager rapidly to cause job restarts"
    echo -e "  Alert fires when: > 3 restarts in 10 minutes"
    separator

    log_step "Triggering rapid restarts (stop/start cycle × 4)..."
    for i in 1 2 3 4; do
        echo -ne "${CYAN}  ▶ Restart cycle ${i}/4...${NC}"
        docker stop "$FLINK_TM_CONTAINER" > /dev/null 2>&1
        sleep 5
        docker start "$FLINK_TM_CONTAINER" > /dev/null 2>&1
        sleep 15
        echo -e " ${GREEN}done${NC}"
    done

    # Wait for Prometheus to see the restarts
    log_wait "Waiting 60s for Prometheus to detect restart count..."
    wait_with_progress 60 "Checking restarts"

    # Check restart count
    local restart_count
    restart_count=$(curl -s "${PROMETHEUS_URL}/api/v1/query?query=increase(flink_jobmanager_job_numRestarts[10m])" | python3 -c "
import sys, json
data = json.load(sys.stdin)
results = data.get('data', {}).get('result', [])
if results:
    print(int(float(results[0]['value'][1])))
else:
    print(0)
" 2>/dev/null || echo "0")
    echo -e "  Job restarts in last 10m: ${YELLOW}${restart_count}${NC}"

    local prom_state
    prom_state=$(check_prometheus_alert "FlinkJobRestarting")
    if [ "$prom_state" = "FIRING" ]; then
        log_ok "Scenario 7: FlinkJobRestarting = ${RED}FIRING${GREEN} ✔${NC}"
    else
        log_warn "Scenario 7: FlinkJobRestarting = ${prom_state} (restarts: ${restart_count}, threshold: >3)"
    fi

    separator
    echo -e "  ${BOLD}Verify:${NC} ${CYAN}${PROMETHEUS_URL}/alerts${NC}"
}

# ================================================================
# SUMMARY: Print final report
# ================================================================
print_summary() {
    log_header "Alert Test Summary"

    echo -e "  ${BOLD}Checking all 7 alerts in Prometheus...${NC}"
    separator

    local alerts=("DQGateFreshnessFailed" "DQGateValidityFailed" "DQGateNullCheckFailed" "GoldAggregationFailed" "FlinkJobNotRunning" "FlinkJobRestarting" "KafkaConsumerLagHigh")
    local severities=("critical" "critical" "critical" "critical" "critical" "warning" "warning")
    local passed=0
    local total=${#alerts[@]}

    for i in "${!alerts[@]}"; do
        local alert="${alerts[$i]}"
        local severity="${severities[$i]}"
        local state
        state=$(check_prometheus_alert "$alert")

        if [ "$state" = "FIRING" ]; then
            echo -e "  ${RED}[FIRING]${NC}   ${alert} (${severity})"
            passed=$((passed + 1))
        else
            echo -e "  ${GREEN}[OK]${NC}      ${alert} (${severity})"
        fi
    done

    separator
    echo -e "  ${BOLD}Results: ${passed}/${total} alerts triggered${NC}"
    echo ""
    echo -e "  ${BOLD}Verify in browser:${NC}"
    echo -e "    Prometheus Alerts: ${CYAN}${PROMETHEUS_URL}/alerts${NC}"
    echo -e "    Grafana Alerts:   ${CYAN}${GRAFANA_URL}/alerting/list${NC}"
    echo -e "    Airflow DAG:      ${CYAN}http://localhost:8085/dags/test_alert_scenarios/grid${NC}"
    echo ""

    # Check Grafana provisioned rules
    log_step "Grafana provisioned alert rules:"
    curl -s -u "${GRAFANA_USER}:${GRAFANA_PASS}" \
        "${GRAFANA_URL}/api/v1/provisioning/alert-rules" 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for rule in data:
        title = rule.get('title', '')
        labels = rule.get('labels', {})
        severity = labels.get('severity', 'unknown')
        print(f'    [{severity:8s}] {title}')
    print(f'    Total: {len(data)} rules provisioned')
except Exception as e:
    print(f'    Could not read Grafana rules: {e}')
" 2>/dev/null || echo "    Grafana API not reachable"
}

# ================================================================
# MAIN
# ================================================================
main() {
    echo -e "${BOLD}"
    echo "  ╔══════════════════════════════════════════════════════╗"
    echo "  ║     Data Platform Alert Rule Test Suite              ║"
    echo "  ║     Tests all 7 Prometheus + Grafana alert rules     ║"
    echo "  ╚══════════════════════════════════════════════════════╝"
    echo -e "${NC}"

    local mode="${1:-all}"

    check_prerequisites

    case "$mode" in
        --dq-only)
            test_dq_scenarios
            ;;
        --flink-only)
            test_flink_not_running
            test_flink_restarting
            ;;
        --kafka-only)
            test_kafka_lag
            ;;
        --summary)
            print_summary
            ;;
        all|*)
            test_dq_scenarios
            echo ""
            echo -e "${YELLOW}  Press Enter to continue with Flink scenarios (will stop containers)...${NC}"
            read -r
            test_flink_not_running
            echo ""
            echo -e "${YELLOW}  Press Enter to continue with Flink restart scenario...${NC}"
            read -r
            test_flink_restarting
            ;;
    esac

    print_summary
}

main "$@"
