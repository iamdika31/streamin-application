package com.bigProject_streaming;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.Producer;

public class RunApp {
	static Logger logger = Logger.getLogger(RunApp.class.getName());

	public static void main(String[] args) {
		
//		 String topic = "twitter-test2";
//		 MainProducer mainproducer = new MainProducer();
//		 MainConsumer MainConsumer = new MainConsumer();
//		 Thread t1 = new Thread(new Runnable() { 
//	            @Override
//	            public void run() 
//	            { 
//	                try { 
//	                	Producer<String,String> producer = ProducerCreator.createProducerFe();
//	                	mainproducer.PushTwittermessage(producer,topic);
//	                } 
//	                catch (InterruptedException | IOException e) { 
//	                	logger.log(Level.WARNING,e.getMessage());
//	                } 
//	            } 
//	        });
//		 Thread t2 = new Thread(new Runnable() { 
//	            @Override
//	            public void run() 
//	            { 
//	                try { 
//	                	MainConsumer.WithSparkStream(topic);
//	                } 
//	                catch (InterruptedException e) { 
//	                	logger.log(Level.WARNING,e.getMessage());
//	                } catch (IOException e) {
//						e.printStackTrace();
//					} 
//	            } 
//	        });
//		 t1.start();
//		 t2.start();
	}

}
