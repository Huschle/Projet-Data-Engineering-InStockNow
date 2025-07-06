#!/bin/bash

# IoT POC Docker Startup Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Detect Docker Compose command
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
elif docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    echo -e "${RED}[ERROR]${NC} Docker Compose not found. Please install Docker Compose."
    exit 1
fi

print_status "Using Docker Compose command: $DOCKER_COMPOSE"

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Function to check if a service is ready
wait_for_service() {
    local service_name=$1
    local container_name=$2
    local timeout=60
    local counter=0
    
    print_step "Waiting for $service_name to be ready..."
    
    while [ $counter -lt $timeout ]; do
        # Check if container is running and healthy
        if $DOCKER_COMPOSE ps | grep -q "${container_name}.*Up"; then
            # For most services, just check if container is up and running
            case $service_name in
                "HDFS NameNode"|"HDFS DataNode")
                    # For HDFS, wait a bit longer and check if healthy
                    if $DOCKER_COMPOSE ps | grep -q "${container_name}.*(healthy)"; then
                        print_status "$service_name is ready!"
                        return 0
                    fi
                    ;;
                *)
                    # For other services, just wait for container to be up
                    print_status "$service_name is ready!"
                    return 0
                    ;;
            esac
        fi
        
        sleep 3
        counter=$((counter + 3))
        echo -n "."
    done
    
    print_warning "$service_name took longer than expected, but continuing..."
    return 0
}

# Function to show help
show_help() {
    echo "IoT POC Docker Management Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  start       Start all services"
    echo "  stop        Stop all services"
    echo "  restart     Restart all services"
    echo "  build       Build all Docker images"
    echo "  logs        Show logs for all services"
    echo "  status      Show status of all services"
    echo "  clean       Stop and remove all containers and images"
    echo "  analysis    Run analysis component (full output)"
    echo "  clean-analysis  Run analysis with clean output (filtered)"
    echo "  help        Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 start           # Start the entire pipeline"
    echo "  $0 logs simulator  # Show logs for simulator service"
    echo "  $0 clean-analysis  # Run analysis with clean output"
    echo "  $0 analysis        # Run analysis with full output"
}

# Function to start infrastructure services
start_infrastructure() {
    print_step "Starting infrastructure services (Kafka, HDFS)..."
    
    $DOCKER_COMPOSE up -d zookeeper kafka namenode datanode
    
    wait_for_service "Zookeeper" "zookeeper"
    wait_for_service "Kafka" "kafka"
    wait_for_service "HDFS NameNode" "namenode"
    wait_for_service "HDFS DataNode" "datanode"
    
    print_status "Infrastructure services are ready!"
}

# Function to start application services
start_applications() {
    print_step "Starting application services..."
    
    # Start services in order
    $DOCKER_COMPOSE up -d simulator
    sleep 10
    
    $DOCKER_COMPOSE up -d alert-filter
    sleep 5
    
    $DOCKER_COMPOSE up -d alert-handler
    sleep 5
    
    $DOCKER_COMPOSE up -d storage
    
    print_status "Application services started!"
}

# Function to run analysis
run_analysis() {
    print_step "Running analysis component..."
    $DOCKER_COMPOSE run --rm analysis
}

# Function to run clean analysis (filtered output)
run_clean_analysis() {
    print_step "Running clean analysis (filtered output)..."
    echo "Analyzing IoT Data..."
    echo "========================"
    
    # Run analysis and filter output to show only meaningful results
    $DOCKER_COMPOSE run --rm analysis 2>/dev/null | \
    grep -E "(ANALYSE|Week-end|Semaine|Top|produits|alertes|stock|heure|===|Total messages|^[0-9])" | \
    head -50
    
    echo ""
    echo "Analysis complete!"
    echo "For full logs, run: ./docker-run.sh analysis"
}

# Main script logic
case ${1:-"help"} in
    "start")
        print_step "Starting IoT POC Pipeline..."
        start_infrastructure
        start_applications
        print_status "IoT POC Pipeline is running!"
        echo ""
        echo "Services available:"
        echo "  - Kafka UI: http://localhost:9092"
        echo "  - HDFS UI: http://localhost:9870"
        echo "  - HDFS DataNode: http://localhost:9864"
        echo ""
        echo "To run clean analysis: $0 clean-analysis"
        echo "To run full analysis: $0 analysis"
        echo "To view logs: $0 logs [service_name]"
        echo "To stop: $0 stop"
        ;;
    
    "stop")
        print_step "Stopping IoT POC Pipeline..."
        $DOCKER_COMPOSE down
        print_status "IoT POC Pipeline stopped!"
        ;;
    
    "restart")
        print_step "Restarting IoT POC Pipeline..."
        $DOCKER_COMPOSE down
        start_infrastructure
        start_applications
        print_status "IoT POC Pipeline restarted!"
        echo ""
        echo "Services available:"
        echo "  - Kafka UI: http://localhost:9092"
        echo "  - HDFS UI: http://localhost:9870"
        echo "  - HDFS DataNode: http://localhost:9864"
        echo ""
        echo "To run clean analysis: $0 clean-analysis"
        echo "To run analysis: $0 analysis"
        echo "To view logs: $0 logs [service_name]"
        echo "To stop: $0 stop"
        ;;
    
    "build")
        print_step "Building Docker images..."
        $DOCKER_COMPOSE build
        print_status "Docker images built successfully!"
        ;;
    
    "logs")
        if [ -n "$2" ]; then
            $DOCKER_COMPOSE logs -f $2
        else
            $DOCKER_COMPOSE logs -f
        fi
        ;;
    
    "status")
        print_step "Checking service status..."
        $DOCKER_COMPOSE ps
        ;;
    
    "clean")
        print_warning "This will stop and remove all containers and images!"
        read -p "Are you sure? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            $DOCKER_COMPOSE down -v --remove-orphans
            $DOCKER_COMPOSE down --rmi all
            print_status "Cleanup completed!"
        fi
        ;;
    
    "analysis")
        run_analysis
        ;;
    
    "clean-analysis")
        run_clean_analysis
        ;;
    
    "help"|*)
        show_help
        ;;
esac
