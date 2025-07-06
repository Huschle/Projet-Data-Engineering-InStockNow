#!/bin/bash

# IoT POC Validation Script
# This script validates that all components are working correctly

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Detect Docker Compose command
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
elif docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    echo -e "${RED}[ERROR]${NC} Docker Compose not found. Please install Docker Compose."
    exit 1
fi

print_test() {
    echo -e "${BLUE}[TEST]${NC} $1"
}

print_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
}

print_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
}

print_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

# Test infrastructure services
test_infrastructure() {
    print_test "Testing infrastructure services..."
    
    # Test Kafka container is running
    if $DOCKER_COMPOSE ps | grep -q "kafka.*Up"; then
        print_pass "Kafka container is running"
    else
        print_fail "Kafka container is not running"
        return 1
    fi
    
    # Test HDFS NameNode
    if curl -s -f "http://localhost:9870/webhdfs/v1/?op=LISTSTATUS" > /dev/null 2>&1; then
        print_pass "HDFS NameNode is responding"
    else
        print_fail "HDFS NameNode is not responding"
        return 1
    fi
    
    # Test HDFS DataNode
    if curl -s -f "http://localhost:9864" > /dev/null 2>&1; then
        print_pass "HDFS DataNode is responding"
    else
        print_fail "HDFS DataNode is not responding"
        return 1
    fi
}

# Test application services
test_applications() {
    print_test "Testing application services..."
    
    # Check if containers are running
    local services=("simulator" "alert-filter" "alert-handler" "storage")
    
    for service in "${services[@]}"; do
        if $DOCKER_COMPOSE ps | grep -q "${service}.*Up"; then
            print_pass "$service container is running"
        else
            print_fail "$service container is not running"
            return 1
        fi
    done
}

# Test Kafka topics
test_kafka_topics() {
    print_test "Testing Kafka topics..."
    
    local topics=("instocknow-input" "instocknow-alerts" "instocknow-missing-products")
    
    for topic in "${topics[@]}"; do
        if $DOCKER_COMPOSE exec -T kafka kafka-topics --list --bootstrap-server localhost:9092 | grep -q "$topic"; then
            print_pass "Topic $topic exists"
        else
            print_info "Topic $topic does not exist yet (this is normal if components haven't created it)"
        fi
    done
}

# Test HDFS data
test_hdfs_data() {
    print_test "Testing HDFS data storage..."
    
    # Wait a bit for data to be stored
    sleep 10
    
    if $DOCKER_COMPOSE exec -T namenode hdfs dfs -ls / | grep -q "stock-data"; then
        print_pass "HDFS stock-data directory exists"
        
        # Check if there's any data
        if $DOCKER_COMPOSE exec -T namenode hdfs dfs -ls /stock-data 2>/dev/null | grep -q "year="; then
            print_pass "HDFS contains partitioned data"
        else
            print_info "HDFS directory exists but no data yet (wait for storage component to process messages)"
        fi
    else
        print_info "HDFS stock-data directory does not exist yet (wait for storage component to create it)"
    fi
}

# Test data flow
test_data_flow() {
    print_test "Testing end-to-end data flow..."
    
    # Check simulator logs for message production (check recent logs)
    if $DOCKER_COMPOSE logs simulator --tail=10 | grep -q "Envoi"; then
        print_pass "Simulator is producing messages"
    else
        print_fail "Simulator is not producing messages"
        return 1
    fi
    
    # Check storage logs for message consumption
    if $DOCKER_COMPOSE logs storage --tail=10 | grep -q "Stocké"; then
        print_pass "Storage is consuming and storing messages"
    else
        print_info "Storage has not stored messages yet (check if Kafka topics are created)"
    fi
}

# Run analysis test
test_analysis() {
    print_test "Testing analysis component..."
    
    print_info "Running analysis component..."
    if $DOCKER_COMPOSE run --rm analysis; then
        print_pass "Analysis component executed successfully"
    else
        print_fail "Analysis component failed to execute"
        return 1
    fi
}

# Main validation
main() {
    echo "========================================="
    echo "IoT POC Validation Script"
    echo "========================================="
    echo
    
    # Check if services are running
    if ! $DOCKER_COMPOSE ps | grep -q "Up"; then
        print_fail "Services are not running. Start them with: ./docker-run.sh start"
        exit 1
    fi
    
    # Run tests
    test_infrastructure
    echo
    
    test_applications
    echo
    
    test_kafka_topics
    echo
    
    test_hdfs_data
    echo
    
    test_data_flow
    echo
    
    #print_info "Waiting 30 seconds for data to accumulate..."
    #sleep 30
    
    # test_analysis
    echo
    
    echo "========================================="
    echo "Validation Complete!"
    echo "========================================="
    echo
    echo "Next steps:"
    echo "1. Monitor logs: ./docker-run.sh logs"
    echo "2. Check HDFS UI: http://localhost:9870"
    echo "3. Run analysis: ./docker-run.sh analysis"
    echo "4. Stop services: ./docker-run.sh stop"
}

main "$@"
