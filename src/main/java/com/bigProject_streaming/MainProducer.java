package com.bigProject_streaming;

/**
 * Hello world!
 *
 */

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.logging.Logger;
import java.util.logging.Level;
import com.bigProject_streaming.getProperties;

public class MainProducer 
{
	//initialization logger
	static Logger logger = Logger.getLogger(MainProducer.class.getName());
	
	
	public static void PushTwittermessage(Producer<String, String> producer,String topic) throws InterruptedException {
		
		Properties props = getProperties.readProperties();
		
		String consumerKey = props.getProperty("consumerKey");
		String consumerSecret = props.getProperty("consumerSecret");
		String token = props.getProperty("token");
		String secret = props.getProperty("secret");
		
		
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        
        //add some track terms
        endpoint.trackTerms(Lists.newArrayList("twitterapi", "spotify"));
        Authentication auth = new OAuth1(consumerKey,consumerSecret,token,secret);
        
      //create a new BasicClient. By default gzip is enabled.

        Client client = new ClientBuilder()
        			   .name("client-1")
        			   .hosts(Constants.STREAM_HOST)
                       .endpoint(endpoint)
                       .authentication(auth)
                       .processor(new StringDelimitedProcessor(queue))
                       .build();
        
        //establish a connection
        client.connect();

        for (int msgRead=0 ; msgRead<20 ; msgRead++) {
        	try {
                String msg = queue.take();
                JsonElement jsonElement = new JsonParser().parse(msg);
                JsonObject jsonObject = jsonElement.getAsJsonObject();
                if(jsonObject.getAsJsonObject("entities").getAsJsonArray("urls").size() > 0 ) {
                    String spotify_url = jsonObject.getAsJsonObject("entities").getAsJsonArray("urls").get(0).getAsJsonObject().get("expanded_url").getAsString();
                    String regex = "\\bopen.spotify.com\\b";
                    Pattern pattern = Pattern.compile(regex);
                    Matcher matcher = pattern.matcher(spotify_url);
                    System.out.println(spotify_url);  
                    if(matcher.find()) {
                        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
                        producer.send(record);
                        logger.log(Level.INFO, "record success send to producer, data:"+record);
                    }
                    else {
                    	logger.log(Level.INFO, "record doesn't contains 'open.spotify.com'");
                    }
                }
                else {
                	logger.log(Level.INFO,"record doesn't have 'urls' object");
                }
             } 
        	catch (InterruptedException e) {
                e.getStackTrace();
                }
        	 
        }
        producer.close();
        client.stop();
	}

    public static void main( String[] args )
    {
		Producer<String,String> producer = ProducerCreator.createProducerFe();
		final String topic="twitter-test";

		try {
			PushTwittermessage(producer,topic);
		}
		catch(InterruptedException e) {
			logger.log(Level.WARNING,e.getMessage());
		}
    }
}
