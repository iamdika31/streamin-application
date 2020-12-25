package com.bigProject_streaming;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.safety.Whitelist;
import org.jsoup.select.Elements;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;




public class Crawling_information {
	static Logger logger = Logger.getLogger(Crawling_information.class.getName());

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
			String full_title = getData.getMetaTag(page, "og:title");
			String title = getData.getMetaTag(page, "twitter:title");
			String description = getData.getMetaTag(page, "og:description");
			String artist = getData.getMetaTag(page, "twitter:audio:artist_name");
			String type = getData.getMetaTag(page, "og:type");
			String twitter_desc = getData.getMetaTag(page, "twitter:description");
			String song_release_date = null;			
			String playlist_created_by =null;
			
			data_spotify.put("title", title);
			data_spotify.put("full_title", full_title);
			data_spotify.put("description", description);
			data_spotify.put("artist", artist);
			data_spotify.put("type", type);
			
			if(type.equals("music.song") || type.equals("music.album")) {
				song_release_date = getData.getMetaTag(page, "music:release_date");
			}
			if(type.equals("music.playlist")) {
				String regex = "\\w+(?=\\s+on)";
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(full_title);
                if(matcher.find()) {
                	playlist_created_by = matcher.group(); 
                }
			}
			
			data_spotify.put("song_release_date", song_release_date);
			data_spotify.put("playlist_created_by",playlist_created_by);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.log(Level.WARNING, "url tidak dapat dibuka "+e.getMessage());
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
        Whitelist whitelist = Whitelist.simpleText();
        String clean = Jsoup.clean(sources, whitelist);
 
        data_tweet.put("spotify_url", spotify_url);
        data_tweet.put("created_at", created_at);
        data_tweet.put("screen_name", screen_name);
        data_tweet.put("name", name);
        data_tweet.put("sources", clean);
 		return data_tweet;
	}
//	public static void main(String[] args) throws IOException {
//		Crawling_information getData = new Crawling_information();
//		String url = "https://open.spotify.com/track/3Zwu2K0Qa5sT6teCCHPShP?si=P8kxSlPpSS2jAduC9Ed0HQ";  //song
//		String url = "https://open.spotify.com/playlist/3ynSZZ9gY42ngUKYKSGPIh?si=RwC-V4KERc2rNkT-yPkmLg";  //playlist
//		String url = "https://open.spotify.com/album/2qehskW9lYGWfYb0xPZkrS?si=bhiqADFdTVSX-VniRbonMw"; //album
//		String url ="https://open.spotify.com/wrapped/share-e4724a8253204bdfb9386015c8a41d93-1080x1920?si=sC9D0J3WSCqH37Ujkg57Og&track-id=3hIjvZ64lMlOQ4iOKUZ1nv&lang=en"; //wrapped
//		String url ="https://open.spotify.com/artist/343YYaA5MSjiZZ5cGyTr4u"; //artist
//		String url = "https://open.spotify.com/playlist/37i9d";

//		JSONObject json = getData.get_information_spotify(url);
//		if(json.size()==0) {
//			System.out.println(true);
//		}
//		System.out.println(json);
//	}

}
