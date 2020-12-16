package com.bigProject_streaming;

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
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
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
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;

//import org.apache.spark.sql.D
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
public class MainConsumer {
	public static String getValue(String msg) {
		Crawling_from_url getData = new Crawling_from_url();	 
		JsonElement jsonElement = new JsonParser().parse(msg);		        
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        String spotify_url = jsonObject.getAsJsonObject("entities").getAsJsonArray("urls").get(0).getAsJsonObject().get("expanded_url").getAsString();
		JSONObject json_spotify = getData.get_information_spotify(spotify_url);
        return json_spotify.toJSONString();
	}
	public static void main(String[] args) throws InterruptedException {
		Crawling_from_url getData = new Crawling_from_url();	 
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("spark stream with kafka").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc,Durations.seconds(8));
		
// 	VERSI 1
//		Map<String, String> kafkaParams = new HashMap<>();
//		kafkaParams.put("bootstrap.servers", "localhost:9092");
//		kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//		kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//		kafkaParams.put("group.id","sparkStreamGroup");
//		kafkaParams.put("auto.offset.reset", "largest");    	
//		kafkaParams.put("enable.auto.commit","true");
		
		
//VERSI 2
		Map<String,Object> kafkaParams = new HashMap<>();
    	kafkaParams.put("bootstrap.servers", "localhost:9092");
    	kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
    	kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
    	kafkaParams.put("group.id", "group1");
    	kafkaParams.put("auto.offset.reset", "latest");    
		

// VERSI 1
//    	Set<String> topics = Collections.singleton("twitter-test");
    	
//VERSI 2    	
		Collection<String> topics = Arrays.asList("twitter-test2");

// VERSI 1		
//	JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,String.class,String.class,StringDecoder.class,StringDecoder.class,kafkaParams,topics);

//		Consumer<String, String> consumer = propsConsumer.createConsumer(topics.toArray()[0].toString());
//		ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
//		for(ConsumerRecord<String, String> record:consumerRecords) {
//			System.out.println("received a message: "+record.value() + " from "+record.topic().toString());
//		}
		
//VERSI 2	
	JavaInputDStream<ConsumerRecord<String, String>> stream =
			KafkaUtils.createDirectStream(
			ssc,
			LocationStrategies.PreferConsistent(),
			ConsumerStrategies.<String, String>Subscribe(topics,
			kafkaParams)
			);

//VERSI 1	
//	JavaDStream<String> inputJsonStream = directKafkaStream.map(new Function<Tuple2<String,String>,String>(){
//		public String call(Tuple2<String,String> message) throws Exception{
//			System.out.println(message._2);
//			return message._2;
//		}
//	});

	
//VERSI 2	
	JavaDStream<String> stream1 = stream.map(
            new Function<ConsumerRecord<String, String>, String>() {
                @Override
                public String call(ConsumerRecord<String, String> r) {
//                	System.out.println(r.value());
                    return r.value();
                }
            }
    );	
	
	
	
//	inputJsonStream.foreachRDD(rdd -> {
//		SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
//		rdd.foreach(record -> System.out.println("ini : "+ record));
//	});

	
// VERSI 1
//	inputJsonStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
//		public void call(JavaRDD<String> rdd) throws Exception {
//			if(!rdd.isEmpty()) {
//			    SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
//			    Dataset<Row> data = spark.read().json(rdd);
//			    data.show();
//			}
//		}
//	});

// VERSI 2 
	stream1.foreachRDD(new VoidFunction<JavaRDD<String>>() {
		public void call(JavaRDD<String> rdd) throws Exception {
//			JavaRDD<Row> rowRDD = rdd.map(new Function<String, Row>() {
//				public Row call(String msg) {
//					Row row = RowFactory.create(msg);
//					return row;
//				}
//			});
			
			List<String> list_rdd = rdd.collect();
		    SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
			if(!rdd.isEmpty()) {
//			rdd.foreach(x -> list_rdd.add("baba"));
			List<String> aa = new ArrayList<>();  	
			list_rdd.forEach(x -> aa.add(getValue(x)));
			JavaRDD<String> rdd2 = sc.parallelize(aa);
		    Dataset<Row> data = spark.read().json(rdd2);
		    data.show();

			
//				System.out.println(list_rdd);
//				JsonElement jsonElement = new JsonParser().parse(list_rdd.get(0).toString());		        
//		        JsonObject jsonObject = jsonElement.getAsJsonObject();
//		        String spotify_url = jsonObject.getAsJsonObject("entities").getAsJsonArray("urls").get(0).getAsJsonObject().get("expanded_url").getAsString();
//        		JSONObject json_spotify = getData.get_information_spotify(spotify_url);
//		        System.out.println(json_spotify);
//				System.out.println(list_rdd.get(0).toString());
			}
//			if(!rdd.isEmpty()) {
//			    Dataset<Row> data = spark.read().json(rdd);
//			    data.show();
//			}
		}
	});
			
		
	ssc.start();
	ssc.awaitTermination();
	
	}

}
