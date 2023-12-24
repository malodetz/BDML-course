# Block 1

```commandline
docker-compose build
docker-compose up -d
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic topic_name --partitions 1 --replication-factor 1
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job_1.py -d  
```