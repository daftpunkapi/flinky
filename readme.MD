# DOCKER
### A. Create Docker container from dockerfile 
docker-compose up -d 

### B. Create topic 'stream1ws' on kafka server
docker exec -it broker kafka-topics --create \
--bootstrap-server localhost:9092 \
--topic stream1ws

<br>

# pip
### A. Upgrade pip3 first
pip3 install --upgrade pip

### B. Install pacakges:
pip3 install -r requirements.txt

### C. Create requirements.txt file 
pip3 freeze > requirements.txt

### D. List all installed libraries 
pip3 list

<br>

# Flink Connector
### A. Download flink-sql-connector-kafka-1.17.0.jar
http --download https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.0/flink-sql-connector-kafka-1.17.0.jar

Make sure the path is correct in the flink scripts