package edu.purdue.eaps.weatherpipe;


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public class INIEditor {
	String path = null;
	FileInputStream in = null;
	FileOutputStream out;
	Properties p;


	public void read(String filename) throws IOException {
		path = filename;
		p = new Properties();
		in = new FileInputStream(path);
		p.load(in);
	}

	public void setValue(String key, String value) throws IOException{
		out = new FileOutputStream(path);
		p.setProperty(key, value);
		p.store(out, null);
		out.close();
	}
	
	public boolean hasKey(String key){
		return p.containsKey(key);
	}
	
	public String getValue(String name) {
		String value = p.getProperty(name);
		return value;
	}
	
}
