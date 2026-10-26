# Запуск продюсера на топик payment-created-events-topic
#!/bin/bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092 \
  --topic payment-created-events-topic \
  --reader-property "print.key=true"

