package com.bigProject_streaming;

import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

public class WriteToElastic {
	static Logger logger = Logger.getLogger(RunApp.class.getName());
	public void toElastic(String msg, String indexName) throws IOException  {
		Properties props = new getProperties().readProperties();
		RestHighLevelClient client = new RestHighLevelClient( RestClient.builder(
		        					 new HttpHost(props.getProperty("HOST"),Integer.parseInt(props.getProperty("PORT")),props.getProperty("TYPE") )));
		
		IndexRequest indexRequest = new IndexRequest(indexName,"_doc").source(msg,XContentType.JSON);
		
		IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
		String id= indexResponse.getId();
//		logger.log(Level.INFO, id);
		
		client.close();
	}
}
