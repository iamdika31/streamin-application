package com.bigProject_streaming;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;

public class mainProducer2 {
	String consumerKey = "UNrdMgapppFrSBap6Vu0YYgWY";
	String consumerSecret = "MAdQYQMTbYxri1oym3qwxTCYABxr6gphPlliTXyLRnLbg9q0wE";
	String token = "433147302-HfxHiAcgWqHoiyjvobAck02XkJbzaUXqHqPqV7sn";
	String secret = "YHJgZX2IBbs6xYVnx4hEQ5XCoyu3ZqIFJRQvTTvwFsrux";
	
	
	BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
    StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
