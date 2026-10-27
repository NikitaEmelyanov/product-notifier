Проект стал мультимодульным, для удобства. Producer и Consumer для удобства собраны в одном месте.
docker-compose с развертыванием кластера Kafka вынесен в корень проекта 

Product service - Produser системы производит и отправляет сообщения в Kafka-broker
Email-notification service - Consumer системы, который считывает сообщения из Kafka-брокера

1) Для сборки и запуска проекта необходимо поднять кластер Кафки в Docker