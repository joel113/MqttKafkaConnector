package com.joel.iot.mqttkafkaconnector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import com.joel.iot.mqttkafkaconnector.mqtt.MqttSource;

public class MqttKafkaSourceTask extends SourceTask {

	private String broker;
	private String mqttTopic;
	private String kafkaTopic;
	private String clientId;
	private MqttSource source;
	
	public String version() {
		return null;
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		ArrayList<SourceRecord> records = new ArrayList<>();
		String line = source.getMqttMessage();
		Map<String,?> sourcePartition = Collections.singletonMap("topic", mqttTopic);
		Map<String,?> sourceOffset = Collections.singletonMap("position", 0);
		records.add(new SourceRecord(sourcePartition, sourceOffset, kafkaTopic, Schema.STRING_SCHEMA, line));
		return records;
	}

	@Override
	public void start(Map<String, String> properties) {
		broker = properties.get(MqttKafkaSourceConnector.BROKER_CONFIG);
		mqttTopic = properties.get(MqttKafkaSourceConnector.MQTT_TOPIC_CONFIG);
		mqttTopic = properties.get(MqttKafkaSourceConnector.KAFKA_TOPIC_CONFIG);
		clientId = properties.get(MqttKafkaSourceConnector.CLIENTID_CONFIG);
		source = new MqttSource(broker, mqttTopic, clientId);
		source.connect();
	}

	@Override
	public void stop() {
		source.disconnect();		
	}

}
