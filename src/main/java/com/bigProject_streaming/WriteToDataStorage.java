package com.bigProject_streaming;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.http.HttpHost;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.JSONObject;

public class WriteToDataStorage {
	static Logger logger = Logger.getLogger(WriteToDataStorage.class.getName());
	
	public void writeData_structured(Dataset<Row> ds) {
		Dataset<Row> SongCategory = ds.select(ds.col("*")).where(ds.col("type").equalTo("music.song")).drop(ds.col("playlist_created_by"));
		Dataset<Row> PlaylistCategory = ds.select(ds.col("*")).where(ds.col("type").equalTo("music.playlist")).drop(ds.col("song_release_date"));
		Dataset<Row> AlbumCategory = ds.select(ds.col("*")).where(ds.col("type").equalTo("music.album")).drop(ds.col("playlist_created_by"));		

		
		toHDFS(SongCategory, "/data_spotify/song.csv","csv");
		toHDFS(PlaylistCategory, "/data_spotify/playlist.csv","csv");
		toHDFS(AlbumCategory, "/data_spotify/album.csv","csv");

		toPostgres(SongCategory,"public.spotify_song");
		toPostgres(PlaylistCategory,"public.spotify_playlist");
		toPostgres(AlbumCategory,"public.spotify_album");
	}
	
	public void toElastic(String msg, String indexName) throws IOException  {
		Properties props = new getProperties().readProperties();
		RestHighLevelClient client = new RestHighLevelClient( RestClient.builder(
		        					 new HttpHost(props.getProperty("HOST"),Integer.parseInt(props.getProperty("PORT")),props.getProperty("TYPE") )));
		
		try {
			IndexRequest indexRequest = new IndexRequest(indexName,"_doc").source(msg,XContentType.JSON);
			
			IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
			String id= indexResponse.getId();
			logger.log(Level.INFO, "success insert into elastic with "+indexName+" "+id);
		}
		catch(Exception e) {
			logger.log(Level.WARNING, "ada kesalahan "+e.getMessage());
		}
		client.close();
	}
	public static void toHDFS(Dataset<Row> ds, String FolderName, String typeFile) {
		try {
			if(ds.count() != 0) {
				if(typeFile.equals("csv")) {
				ds.write().format("csv").option("header", "true").mode("append").option("sep",";").save(FolderName);
				logger.log(Level.INFO, "data success input to HDFS with folder name: "+FolderName);
				}
				else if(typeFile.equals("json")) {
					ds.write().mode("append").json(FolderName);
					logger.log(Level.INFO, "data success input to HDFS with folder name: "+FolderName);				
				}
			}
			else {
				logger.log(Level.INFO, "dataset is empty");
			}
		}catch(Exception e) {
			logger.log(Level.WARNING,e.getMessage());
		}
	}
	
	public static void toPostgres(Dataset<Row> ds, String dbTable) {		
		try {
			if(ds.count()!= 0) {
					ds.write().mode("append").format("jdbc")
								.option("url","jdbc:postgresql://localhost:5432/data_spotify")
								.option("dbtable",dbTable)
								.option("user", "postgres")
								.option("password", "root").save();
					
			//		ds.show();
					logger.log(Level.INFO, "data success input to POSTGRES with tablename "+dbTable);
			}
			else {
				logger.log(Level.INFO,"dataset is empty");
			}
		}catch(Exception e) {
			logger.log(Level.WARNING,e.getMessage());
		}
	}	
}
