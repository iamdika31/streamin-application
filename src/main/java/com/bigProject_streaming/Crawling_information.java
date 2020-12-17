package com.bigProject_streaming;

import java.io.IOException;

import org.json.simple.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Crawling_information {

	String getMetaTag(Document document, String attr) {	    
	    Elements elements = document.select("meta[property=" + attr + "]");
	    for (Element element : elements) {
	        final String s = element.attr("content");
	        if (s != null) return s;
	    }
	    return null;
	}
	
	public JSONObject get_information_spotify(String url) {
		Crawling_information getData = new Crawling_information();
		JSONObject data_spotify = new JSONObject();
		try {
			Document page = Jsoup.connect(url)
					.get();
//			System.out.println(page.title());
			String title = getData.getMetaTag(page, "og:title");
			String description = getData.getMetaTag(page, "og:description");
			String artist = getData.getMetaTag(page, "twitter:audio:artist_name");
			String type = getData.getMetaTag(page, "og:type");
			String twitter_desc = getData.getMetaTag(page, "twitter:description");
						
			data_spotify.put("title", page.title());
			data_spotify.put("full_title", title);
			data_spotify.put("description", description);
			data_spotify.put("artist", artist);
			data_spotify.put("type", type);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return data_spotify;
	}
	
	
	public static JSONObject get_information_tweets(String tweet) {
		JSONObject data_tweet = new JSONObject();

		JsonElement jsonElement = new JsonParser().parse(tweet);
        
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        String spotify_url = jsonObject.getAsJsonObject("entities").getAsJsonArray("urls").get(0).getAsJsonObject().get("expanded_url").getAsString();
        String created_at = jsonObject.get("created_at").getAsString();
        String screen_name = jsonObject.getAsJsonObject("user").get("screen_name").getAsString();
        String name = jsonObject.getAsJsonObject("user").get("name").getAsString();
        String sources = jsonObject.get("source").getAsString();
        data_tweet.put("spotify_url", spotify_url);
        data_tweet.put("created_at", created_at);
        data_tweet.put("screen_name", screen_name);
        data_tweet.put("name", name);
        data_tweet.put("sources", sources);
         
		return data_tweet;
	}
//	public static void main(String[] args) {
//		Crawling_from_url getData = new Crawling_from_url();
//		String url = "https://open.spotify.com/user/bighitktm/playlist/0EzsHoXrDojeNignJqH1QO?si=JxAEAdQ1SyumiXsIUysGSg";  
//		JSONObject json = getData.get_information_spotify(url);
//		System.out.println(json);
//	}

}
