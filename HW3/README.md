
Всегда запускаем consumer в отдельном терминале

# Block 1

1&2. 
```commandline
docker-compose build
docker-compose up -d
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic itmo2023 --partitions 1 --replication-factor 1
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic itmo2023_processed --partitions 1 --replication-factor 1
```
3.
```commandline
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job_1.py -d 
python producer_1.py
python consumer_1.py
```
4.
```commandline
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job_2.py -d 
python producer_1.py
python consumer_1.py
```
5. В папке screenshots

# Block 2

1&2. 
```commandline
docker-compose build
docker-compose up -d
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic itmo2023 --partitions 1 --replication-factor 1
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic itmo2023_processed --partitions 1 --replication-factor 1
```
3.
```commandline
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job_3.py -d 
python producer_1.py
python consumer_1.py
```
4.
```commandline
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job_4.py -d 
python producer_1.py
python consumer_1.py
```
5.
```commandline
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job_5.py -d 
python producer_1.py
python consumer_1.py
```
# Block 3
1&2. 
```commandline
docker-compose build
docker-compose up -d
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic itmo2023 --partitions 1 --replication-factor 1
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic itmo2023_processed --partitions 1 --replication-factor 1
```
3.
```commandline
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job.py -d 
python producer_1.py
python consumer_2.py
```