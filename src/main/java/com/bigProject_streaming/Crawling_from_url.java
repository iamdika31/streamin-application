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

public class Crawling_from_url {

	String getMetaTag(Document document, String attr) {	    
	    Elements elements = document.select("meta[property=" + attr + "]");
	    for (Element element : elements) {
	        final String s = element.attr("content");
	        if (s != null) return s;
	    }
	    return null;
	}
	
	public JSONObject get_information_spotify(String url) {
		Crawling_from_url getData = new Crawling_from_url();
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
//	public static void main(String[] args) {
//		Crawling_from_url getData = new Crawling_from_url();
//		String url = "https://open.spotify.com/user/bighitktm/playlist/0EzsHoXrDojeNignJqH1QO?si=JxAEAdQ1SyumiXsIUysGSg";  
//		JSONObject json = getData.get_information_spotify(url);
//		System.out.println(json);
//	}

}
