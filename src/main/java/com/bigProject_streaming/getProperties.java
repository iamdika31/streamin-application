package com.bigProject_streaming;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.glassfish.hk2.utilities.reflection.Logger;

public class getProperties {
	public static Properties readProperties() {
		Properties prop = new Properties();
		Path mypath = Paths.get("src/main/resources/project.properties");
		try {
			BufferedReader bf = Files.newBufferedReader(mypath,StandardCharsets.UTF_8);
			prop.load(bf);
		}
		catch(IOException ie) {
			ie.printStackTrace();
		}
		return prop;
	}
}
