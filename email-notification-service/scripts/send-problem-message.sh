#!/bin/bash
# Отправка сообщений в топик product-created-events-topic
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092 \
  --topic product-created-events-topic \
  --reader-property "parse.key=true" \
  --reader-property "key.separator=:"