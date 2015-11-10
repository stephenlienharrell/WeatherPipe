package edu.purdue.cs307.team16;

import java.io.IOException;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


import org.apache.commons.codec.binary.Base64;


public class Reduce extends Reducer<Text, Text, Text, Text> {	
	
	Integer passNum = 0;
	ResearcherMapReduceAnalysis analysis = null;
	
	public void reduce(Text keyname, Iterable<Text> str, Context context) throws IOException, InterruptedException {
		if(analysis == null) analysis = new ResearcherMapReduceAnalysis();
    	
    	for(Text val : str) {
    		byte[] dataByte = null;
    		MapReduceSerializer obj;
    		dataByte = Base64.decodeBase64(val.toString());
    		obj = (MapReduceSerializer) SerializationUtils.deserialize(dataByte);
    		analysis.reduce(obj.serializeMe);
    		passNum++;
    	}
    	
       	byte[] databyte = SerializationUtils.serialize(analysis.serializer);      	
       	String byte_to_string = Base64.encodeBase64String(databyte);
       	
    	context.write(new Text("Run#" + passNum.toString()), new Text(byte_to_string));

    }

}
