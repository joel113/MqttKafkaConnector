package com.joel.iot.mqttkafkaconnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.log4j.Logger;

public class MqttKafkaSourceConnector extends SourceConnector {
	
	private String broker;
	private String mqttTopic;
	private String kafkaTopic;
	private String clientId;
	private static Logger log = Logger.getLogger("MqttKafkaSourceConnector");
	
	public static final String BROKER_CONFIG = "broker_config";
	public static final String MQTT_TOPIC_CONFIG = "mqtt_topic_config";
	public static final String KAFKA_TOPIC_CONFIG = "kafka_topic_config";
	public static final String CLIENTID_CONFIG = "clientid_config";
	
	private static final ConfigDef CONFIG_DEFINTION = new ConfigDef()
			.define(BROKER_CONFIG, Type.STRING, Importance.HIGH, "MQTT Broker Connection String")
			.define(MQTT_TOPIC_CONFIG, Type.STRING, null, Importance.HIGH, "MQTT Topic Name")
			.define(KAFKA_TOPIC_CONFIG, Type.STRING, null, Importance.HIGH, "KAFKA Topic Name")
			.define(CLIENTID_CONFIG, Type.STRING, null, Importance.HIGH, "Client ID Name");

	@Override
	public ConfigDef config() {
		return CONFIG_DEFINTION;
	}

	@Override
	public void start(Map<String, String> properties) {
		broker = properties.get(BROKER_CONFIG);
		mqttTopic = properties.get(MQTT_TOPIC_CONFIG);
		kafkaTopic = properties.get(KAFKA_TOPIC_CONFIG);
		clientId = properties.get(CLIENTID_CONFIG);
		log.info(String.format("Read connector with arguments %s, %s, %s and %s.", broker, mqttTopic, kafkaTopic, clientId));
	}

	@Override
	public void stop() {
		// nothing to do here		
	}

	@Override
	public Class<? extends Task> taskClass() {
		return MqttKafkaSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		ArrayList<Map<String,String>> configs = new ArrayList<>();
		Map<String,String> config = new HashMap<>();
		if(broker != null) {
			config.put(BROKER_CONFIG, broker);
		}
		if(mqttTopic != null) {
			config.put(MQTT_TOPIC_CONFIG, mqttTopic);
		}
		if(kafkaTopic != null) {
			config.put(KAFKA_TOPIC_CONFIG, kafkaTopic);
		}
		if(clientId != null) {
			config.put(CLIENTID_CONFIG, clientId);
		}
		configs.add(config);
		return configs;
	}

	@Override
	public String version() {
		return null;
	}

}
