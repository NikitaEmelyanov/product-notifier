#!/bin/bash
# Чтение сообщений из Dead Letter Topic (DLT)
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092 \
  --topic product-created-events-topic-dlt \
  --formatter-property "print.key=true"