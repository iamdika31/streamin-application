package com.bigProject_streaming;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;



public class ProducerCreator {
	public static Producer<String,String> createProducerFe(){
		Properties props = new Properties();
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092" );
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer1");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<>(props);

	}
}
