package com.developer.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	final Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class);
	private String consumerKey = "EyY432pYGyWvCHfZGVe6WBlhO";
	private String consumerSecret = "phfTWeULcji0CBFHhbtLTJHtLSN3QBSqLgXVN3RPPDY2aPPStw";
	private String token = "352022448-0hi6AQYUhgxQIs2ZbobhTJr4M5IaQ4c6bpRs4tAH";
	private String secret = "PYQjveWoFusRevBT9e50lRcLEUqZ0mLdJPX7rG4yX4NZ6";
	List<String> terms = Lists.newArrayList("kafka");
	Client client = null;
	KafkaProducer<String, String> producer = null;

	public static void main(String[] args) {

		new TwitterProducer().run();
	}

	public void run() {
		LOGGER.info("Setup");

		// create twitter client
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
		client = createTwitterClient(msgQueue);
		client.connect();

		// create kafka producer
		producer = createKafkaProducer();

		// add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			LOGGER.info("Stopping application");
			LOGGER.info("shutting down client from twitter....");
			client.stop();
			
			LOGGER.info("closing producer....");
			producer.close();
			
			LOGGER.info("Done!!!!");
		}));

		// loop to send tweets to kafka
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				client.stop();
			}

			if (msg != null) {
				LOGGER.info(msg);
				producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if (exception != null)
							LOGGER.error("Something bad happened.", exception);

					}
				});
			}

		}
		LOGGER.info("End of application");
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		// List<Long> followings = Lists.newArrayList(1234L, 566788L);

		// hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
		// .eventMessageQueue(eventQueue); // optional: use this if you want to process
		// client events

		Client hosebirdClient = builder.build();
		// Attempts to establish a connection.
		return hosebirdClient;
	}

	public KafkaProducer<String, String> createKafkaProducer() {
		String bootstrapServer = "127.0.0.1:9092";

		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		return producer;
	}

}
