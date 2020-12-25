package com.bigProject_streaming;


import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.logging.Logger;
import java.util.logging.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka.KafkaUtils;

//import kafka.serializer.StringDecoder;
import scala.Tuple2;

import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.simple.JSONObject;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;


class JavaSparkSessionSingleton {
    private static transient SparkSession instance = null;
    public static SparkSession getInstance(SparkConf sparkConf) {
		String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
      if (instance == null) {
        instance = SparkSession
          .builder()
          .config(sparkConf)
          .getOrCreate();
      }
      return instance;
    }
  }

class getInformation{
	//initialization logger
	static Logger logger = Logger.getLogger(MainConsumer.class.getName());
	
	public static String getJSONInformation(String message) {
		
		Crawling_information getData = new Crawling_information();
		JSONObject json_tweet_append = new JSONObject();
		JSONObject json_spotify = new JSONObject();
		try {
			JsonElement jsonElement = new JsonParser().parse(message);		        
	        JsonObject jsonObject = jsonElement.getAsJsonObject();
	        String spotify_url = jsonObject.getAsJsonObject("entities").getAsJsonArray("urls").get(0).getAsJsonObject().get("expanded_url").getAsString();
			json_spotify = getData.get_information_spotify(spotify_url);
			json_tweet_append = getData.get_information_tweets(message);
			json_tweet_append.putAll(json_spotify);
		}
		catch(Exception e){
			logger.log(Level.WARNING, "ada kesalahan "+e.getMessage());
		}
		
        return json_tweet_append.toJSONString();
	}
	
}
class returnConsumerRecord implements Function<ConsumerRecord<String, String>,String>{
	public String call(ConsumerRecord<String, String> cr) {
		return cr.value();
	}
}

class LoopAndPassingRDD{
	public void call(JavaRDD<String> rdd,JavaSparkContext jsc ) throws Exception{
		Logger logger = Logger.getLogger(MainConsumer.class.getName());
		WriteToDataStorage writeToStorage = new WriteToDataStorage();
		List<String> list_rdd = rdd.collect();
	    SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
		if(!rdd.isEmpty()) {
			try {
			//save raw data to hdfs
			JavaRDD<String> list_rdd_parallize = jsc.parallelize(list_rdd);
			Dataset<Row> data_mentah = spark.read().json(list_rdd_parallize);		
		    writeToStorage.toHDFS(data_mentah,"/data_mentah/","json");		

			//save data to postgres and hive						
			List<String> tempVariable = new ArrayList<>();  	
			list_rdd.forEach(x -> tempVariable.add(getInformation.getJSONInformation(x)));			
			JavaRDD<String> sample_data_spotifyRDD = jsc.parallelize(tempVariable);
		    Dataset<Row> data = spark.read().json(sample_data_spotifyRDD);
		    writeToStorage.writeData_structured(data);
//		    data.show();
//		    spark.close();
			}
			catch(Exception e) {
				logger.log(Level.WARNING, "ada kesalahan :" + e.getMessage());
			}
		}
	}
}
public class MainConsumer {
	static Logger logger = Logger.getLogger(MainConsumer.class.getName());

	public static void WithSparkStream(String topic) throws InterruptedException, IOException {
		
		getInformation getInformation = new getInformation();

		Collection<String> topics = Arrays.asList(topic);

		SparkConf conf = new SparkConf().setAppName("spark stream with kafka").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc,Durations.seconds(5));
				
		
		Map<String,Object> kafkaParams =  propsConsumer.kafkaParams(); 

				
		JavaInputDStream<ConsumerRecord<String, String>> stream =
				KafkaUtils.createDirectStream(
				ssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics,
				kafkaParams)
				);
		
		//to return consumer value (message)
		JavaDStream<String> stream1 = stream.map(new returnConsumerRecord());	
		
		//looping each message in rdd
		stream1.foreachRDD(rdd -> new LoopAndPassingRDD().call(rdd, sc));
							
		ssc.start();
		ssc.awaitTermination();
	
	}
	
	
	public static void OnlyKafka(String topic, String indexName1,String indexName2)throws InterruptedException, IOException{
		WriteToDataStorage writeToDataStorage = new WriteToDataStorage();
		try(Consumer<String, String> consumer = propsConsumer.createConsumer(topic)){
			while(true) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
				for(ConsumerRecord<String, String> record:consumerRecords) {
					try {
						//save raw data into elasticsearch
						writeToDataStorage.toElastic(record.value(),indexName1);
						String json_tweet = getInformation.getJSONInformation(record.value());
						System.out.println(json_tweet);

						//save new information data to elastic
						writeToDataStorage.toElastic(json_tweet,indexName2);
					} catch (Exception e) {
						logger.log(Level.WARNING, "ada error "+e.getMessage());
					}
				}
			}
		}
		catch(Exception e) {
			logger.log(Level.WARNING, "ada error "+e.getMessage());
		}
	}
	
	
	
	public static void main(String[] args) throws InterruptedException, IOException{
		 Thread t1 = new Thread(new Runnable() { 
	            @Override
	            public void run() 
	            { 
	                try { 
	            		OnlyKafka("twitter-test2", "data_twitter", "data_spotify");
	                } 
	                catch (InterruptedException | IOException e) { 
	                	logger.log(Level.WARNING,e.getMessage());
	                } 
	            } 
	        });
		 Thread t2 = new Thread(new Runnable() { 
	            @Override
	            public void run() 
	            { 
	                try { 
	            		WithSparkStream("twitter-test2");
	                } 
	                catch (InterruptedException e) { 
	                	logger.log(Level.WARNING,e.getMessage());
	                } catch (IOException e) {
						e.printStackTrace();
					} 
	            } 
	        });
		 t2.start();
		 t1.start();
		 
		 t2.join();
		 t1.join();
		
	}
}
