Kafka Connector for transporting MQTT messages to Kafka. The implementation is developed with insight from the [Kafka Connect documentation](http://docs.confluent.io/current/connect/intro.html) and [Kafka Connect Development Guide](http://docs.confluent.io/current/connect/devguide.html#core-concepts-and-apis).

Commands:
=========

Start the Kafka MQTT Connector:

    ./connect-standalone worker-mqtt.properties quickstart-mqtt.properties
    
Listen to the Kafka Topic if messages are received:
    
    ./kafka-console-consumer --zookeeper localhost:2181 --topic abcd