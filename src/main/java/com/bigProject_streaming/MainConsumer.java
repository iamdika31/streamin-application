package com.bigProject_streaming;

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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
	static Logger logger = Logger.getLogger(MainProducer.class.getName());
	
	public static String getJSONInformation(String message) {
		
		Crawling_information getData = new Crawling_information();
		WriteToElastic writeElastic = new WriteToElastic();
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
			logger.error(e);;
		}
		
		try {
			writeElastic.toElastic(message,"data_twitter");
			writeElastic.toElastic(json_tweet_append.toJSONString(),"data_spotify");
		} catch (IOException e) {
			logger.error(e);
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
		List<String> list_rdd = rdd.collect();
	    SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
		if(!rdd.isEmpty()) {
			List<String> tempVariable = new ArrayList<>();  	
			list_rdd.forEach(x -> tempVariable.add(getInformation.getJSONInformation(x)));
			JavaRDD<String> sample_data_spotifyRDD = jsc.parallelize(tempVariable);
		    Dataset<Row> data = spark.read().json(sample_data_spotifyRDD);
		    data.show();
		}
	}
}
public class MainConsumer {

	public static void mains(String topic) throws InterruptedException, IOException {
		
		getInformation getInformation = new getInformation();

		Collection<String> topics = Arrays.asList(topic);

		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("spark stream with kafka").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc,Durations.seconds(5));
				
		
		Map<String,Object> kafkaParams = propsConsumer.kafkaParams(); 

				
		JavaInputDStream<ConsumerRecord<String, String>> stream =
				KafkaUtils.createDirectStream(
				ssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics,
				kafkaParams)
				);
		
		JavaDStream<String> stream1 = stream.map(new returnConsumerRecord());	
		
		stream1.foreachRDD(rdd -> new LoopAndPassingRDD().call(rdd, sc));
							
		ssc.start();
		ssc.awaitTermination();
	
	}
	public static void main(String[] args) throws InterruptedException, IOException{
		mains("twitter-test2");
	}
}
