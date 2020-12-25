package com.readData;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class readData {

	public static void main(String[] args) {
		String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
		SparkConf conf = new SparkConf().setAppName("spark stream with kafka").setMaster("local[*]");
	    String env = "yarn"; // ini isinya "yarn"
		SparkSession spark = SparkSession.builder()
										.appName("read data")
										.master(env)
										.config("spark.sql.warehouse.dir",warehouseLocation)
										.config("spark.sql.catalogImplementation", "in-memory")
										.enableHiveSupport()
										.getOrCreate();
		
		spark.sql("CREATE TABLE IF NOT EXISTS data_spotify.spotify_song(artist STRING, created_at STRING,description STRING, "
				+ "full_title STRING, screen_name STRING, song_release_date STRING, sources STRING, spotify_url STRING, "
				+ "title STRING, type STRING, name STRING) row format delimited fields terminated by ';'");
		spark.close();
//		spark.sql("LOAD DATA INPATH '/data_spotify/spotify_song/*' overwrite into table data_spotify.spotify_song");
//		spark.sql("SELECT * FROM spotify_song");
	}

}
