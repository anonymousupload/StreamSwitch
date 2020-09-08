echo 0 > /home/myc/workspace/flink-related/flink-testbed/src/main/resources/err.txt
/home/myc/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink_metrics
