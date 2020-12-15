package com.bigProject_streaming;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import com.bigProject_streaming.getProperties;



public class ProducerCreator {
	public static Producer<String,String> createProducerFe(){
		Properties project_props = getProperties.readProperties();
		Properties props = new Properties();
		props.put(ProducerConfig.ACKS_CONFIG, project_props.getProperty("ACKS_VALUE") );
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, project_props.getProperty("BOOTSTRAP_SERVERS"));
		props.put(ProducerConfig.CLIENT_ID_CONFIG, project_props.getProperty("CLIENT_ID"));
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<>(props);

	}
}
