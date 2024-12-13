#requires -PSEdition Core

$env:DOCKER_CLI_HINTS = "false"

docker exec -ti kafka /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server :9092