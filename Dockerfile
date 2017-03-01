FROM nagasuga/docker-hive:latest
MAINTAINER Zachary Rank
COPY a3-dataset/ /a3-dataset/
COPY MapReduce.jar /MapReduce
ENTRYPOINT java -jar /MapReduce &&  cat /output.txt
