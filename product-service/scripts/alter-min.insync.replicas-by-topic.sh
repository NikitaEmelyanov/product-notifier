  #!/bin/bash
  # Установка min.insync.replicas=1 для топика product-created-events-topic
  docker exec -it kafka-1 /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092 \
    --alter \
    --entity-type topics \
    --entity-name product-created-events-topic \
    --add-config min.insync.replicas=2