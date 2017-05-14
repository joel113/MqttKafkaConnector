package com.joel.iot.mqttkafkaconnector.mqtt;

import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MqttSource implements MqttCallback {

	private String broker;
	private String topic;
	private String clientId;
	private MqttClient client;
	private LinkedList<MqttMessage> messages;
	private static Logger LOG = Logger.getLogger("MqttSubject");

	public MqttSource(String broker, String clientId, String topic) {
		this.broker = broker;
		this.clientId = clientId;
		this.topic = topic;
		this.messages = new LinkedList<>();
	}

	public void connect() {
		try {
			MemoryPersistence persistence = new MemoryPersistence();
			LOG.info(String.format("Mqtt connect to broker %s and topic %s.", broker, topic));
			client = new MqttClient(broker, clientId, persistence);
			MqttConnectOptions options = new MqttConnectOptions();
			options.setCleanSession(MqttConnectOptions.CLEAN_SESSION_DEFAULT);
			client.connect(options);
			client.subscribe(topic);
			client.setCallback(this);
		} catch (MqttException ex) {
			LOG.error(String.format("Mqtt connect exception: %", ex.getMessage()));
		}
	}
	
	public void disconnect() {
		try {
			client.disconnect();
		} catch (MqttException ex) {
			LOG.error(String.format("Mqtt connect exception: %", ex.getMessage()));
		}
	}

	public void publish(String message) {
		MqttMessage mqttMessage = new MqttMessage(message.getBytes());
		try {
			if (!client.isConnected()) {
				client.connect();
			}
			client.publish(this.topic, mqttMessage);
		} catch (MqttPersistenceException ex) {
			LOG.error(String.format("Mqtt publish exception: %", ex.getMessage()));
		} catch (MqttException ex) {
			LOG.error(String.format("Mqtt publish exception: %", ex.getMessage()));
		}
	}

	public void connectionLost(Throwable cause) {
		try {
			client.connect();
		} catch (MqttSecurityException ex) {
			LOG.error(String.format("Mqtt connection lost: %", ex.getMessage()));
		} catch (MqttException ex) {
			LOG.error(String.format("Mqtt connection lost: %", ex.getMessage()));
		}
	}

	public void messageArrived(String topic, MqttMessage message) throws Exception {
		LOG.trace("Put a new message into the queue.");
		messages.addLast(message);
	}

	public void deliveryComplete(IMqttDeliveryToken token) {
		LOG.info("onCompleted");
	}
	
	public String getMqttMessage() {
		MqttMessage message;
		LOG.trace("Trying to get a mqtt message.");
		do {
			message = messages.pollFirst();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} while(message == null);
		LOG.trace("Got a mqtt messages.");
		return message.toString();
	}
	
}
