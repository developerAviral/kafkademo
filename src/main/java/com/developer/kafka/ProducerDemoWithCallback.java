package com.developer.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {
	public static void main(String[] args) {
		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

		String bootstrapServer = "127.0.0.1:9092";

		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create producer record
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic",
				"Hi Developer!! I love you.");

		// create producer
		Producer<String, String> producer = new KafkaProducer<>(properties);

		// send data - async call
		producer.send(producerRecord, new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// executes everytime when record sent successfully or exception occurs

				if (exception == null) {
					// record sent successfully
					logger.info("Received new metadata. \n" + "Topic: " + metadata.topic() + "\n" + "Partition: "
							+ metadata.partition() + "\n" + "Offset: " + metadata.offset() + "\n" + "Timestamp: "
							+ metadata.timestamp());
				} else {
					logger.error("Error occured." + exception.getMessage());
				}
			}
		});

		// flush data
		producer.flush();

		// close producer
		producer.close();
	}
}
