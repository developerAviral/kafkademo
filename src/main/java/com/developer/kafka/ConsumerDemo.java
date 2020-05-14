package com.developer.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {
	
	public static void main(String[] args) {
		final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class);

		 String bootstrapServer = "127.0.0.1:9092";
		 String groupId = "my-fourth-application";
		
		//create consumer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest/ latest/ none
		
		//create consumer
		KafkaConsumer< String, String> kafkaConsumer = new KafkaConsumer<>(properties);
		
		//subscribe consumer to topic
		kafkaConsumer.subscribe(Arrays.asList("first_topic"));
		
		//poll for new data
		
		while(true) {
			ConsumerRecords<String, String> consumerRecords =  kafkaConsumer.poll(Duration.ofMillis(100));
			for(ConsumerRecord consumer : consumerRecords) {
				LOGGER.info("Key: " + consumer.key() + " Value: " +consumer.value());
				LOGGER.info("Partition: " + consumer.partition() + " Offset: " +consumer.offset());
				
			}
		}
		
	}
	
}
