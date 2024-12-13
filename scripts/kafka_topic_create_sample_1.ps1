#requires -PSEdition Core

$env:DOCKER_CLI_HINTS = "false"

docker exec -ti kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server :9092 --topic sample.1