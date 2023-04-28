## Create Docker container from dockerfile 
docker-compose up -d 

## Create topic 'stream1ws' on kafka server
docker exec -it broker kafka-topics --create \
--bootstrap-server localhost:9092 \
--topic stream1ws

## Upgrade pip3 first
pip3 install --upgrade pip

## Install pacakges:
pip3 install -r requirements.txt

## Create requirements.txt file 
pip3 freeze > requirements.txt

## List all installed libraries 
pip3 list

## Download flink-sql-connector-kafka-1.17.0.jar
http --download https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.0/flink-sql-connector-kafka-1.17.0.jar