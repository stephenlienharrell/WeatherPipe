package edu.purdue.eaps.weatherpipe.weatherpipemapreduce;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


import org.apache.commons.codec.binary.Base64;


public class Reduce extends Reducer<Text, Text, Text, Text> {	
	
	Integer passNum = 0;
	ResearcherMapReduceAnalysis analysis = null;
	
	public void reduce(Text keyname, Iterable<Text> str, Context context) throws IOException, InterruptedException {
		if(analysis == null) analysis = new ResearcherMapReduceAnalysis(context.getConfiguration());
    	
    	for(Text val : str) {	
    		if(!(analysis.reduce(val.toString()) == true)) continue;
    		passNum++;
    	}
//    	System.out.println("Final array: " + Arrays.toString((double[])analysis.serializer.serializeMe));
    	
       	byte[] databyte = SerializationUtils.serialize(analysis.serializer);      	
       	String byte_to_string = Base64.encodeBase64String(databyte);
       	
    	context.write(new Text("Run#" + passNum.toString()), new Text(byte_to_string));

    }

}
