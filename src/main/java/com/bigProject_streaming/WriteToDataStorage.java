package com.bigProject_streaming;

import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.http.HttpHost;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

public class WriteToDataStorage {
	static Logger logger = Logger.getLogger(RunApp.class.getName());
	
	public void toElastic(String msg, String indexName) throws IOException  {
		Properties props = new getProperties().readProperties();
		RestHighLevelClient client = new RestHighLevelClient( RestClient.builder(
		        					 new HttpHost(props.getProperty("HOST"),Integer.parseInt(props.getProperty("PORT")),props.getProperty("TYPE") )));
		
		IndexRequest indexRequest = new IndexRequest(indexName,"_doc").source(msg,XContentType.JSON);
		
		IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
		String id= indexResponse.getId();
		logger.log(Level.INFO, id);
		
		client.close();
	}
	public static void toHive(Dataset<Row> ds) {
		Dataset<Row> SongCategory = ds.select(ds.col("*")).where(ds.col("type").equalTo("music.song")).drop(ds.col("playlist_created_by"));
		Dataset<Row> PlaylistCategory = ds.select(ds.col("*")).where(ds.col("type").equalTo("music.playlist")).drop(ds.col("song_release_date"));
		Dataset<Row> AlbumCategory = ds.select(ds.col("*")).where(ds.col("type").equalTo("music.album")).drop(ds.col("playlist_created_by"));		
		SongCategory.show();
		PlaylistCategory.show();
		AlbumCategory.show();
	}
//	public static void main(String[] args) {
//		SparkSession spark = SparkSession
//			    .builder()
//			    .master("local")
//			    .appName("Java Spark SQL Example")
//			    .getOrCreate();
//		
//				
//		Dataset<Row> df = spark.read()
//			    .option("mode", "DROPMALFORMED")
//			    .option("delimiter",",")
//			    .option("header","true")
//			    .option("inferSchema","true")
//			    .csv("file:///home/aryadika/example.csv");
//		
//		toHive(df);
//	}
}
