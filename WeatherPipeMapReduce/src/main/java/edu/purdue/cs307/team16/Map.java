package edu.purdue.cs307.team16;

import java.io.IOException;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/*
    Input is of the form:
    ("bucketname", "key")
    ("bucketname", "key")
    ("bucketname", "key")

    At this point, I assume they are in the same bucket. 
*/

public class Map extends Mapper<Text, Text, Text, IntWritable> {
	
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    

    public void map(Text bucketname, Text keyname, Context context) throws IOException, InterruptedException {
    	
    
        String key = keyname.toString();
        // load file from s3 based on the bucket/key name
        // load it in to netcdf
        // grab a simple data piece
        // we will set word to be the key, and one will be set to the data point we pulled
        try {
            word.set(key);
            one.set(1);
            context.write(word, one);
        }
        catch (Exception e) {
            // Send signal

        }
    }
}
