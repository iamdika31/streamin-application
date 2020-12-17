package com.bigProject_streaming;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

//import org.glassfish.hk2.utilities.reflection.Logger;

public class getProperties {
	InputStream input;
	public Properties readProperties() throws IOException {
		Properties prop = new Properties();

		try {
			String propsName = "project.properties";
			input = getClass().getClassLoader().getResourceAsStream(propsName);
			if (input != null) {
				prop.load(input);
			}
			else {
				throw new FileNotFoundException(propsName + " Not Found in the classpath");
			}			
		}
		catch(IOException ie) {
			ie.printStackTrace();
		}
		return prop;
	}
}
