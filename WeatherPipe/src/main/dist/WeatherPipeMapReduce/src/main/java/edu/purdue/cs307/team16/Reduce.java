package edu.purdue.cs307.team16;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.commons.codec.binary.BaseNCodec;
import org.apache.commons.codec.binary.Base64;

import edu.purdue.cs307.team16.MapReduceArraySerializer;

public class Reduce extends Reducer<Text, Text, Text, Text> {	

	public int runningSum;
	public int numberOfDataPoints = 0;
	
	
    public void reduce(Text keyname, Iterable<Text> str, Context context) throws IOException, InterruptedException {
        
    	
    	// add data to runningSum and increment number of data points
    	// for the average
   
        /*for (IntWritable val : values) {
            runningSum += val.get();
            numberOfDataPoints++;
        }
        int average = runningSum/numberOfDataPoints;
        context.write(new Text("Average"), new IntWritable(average));*/
    	
    	
    	for(Text val : str) {
    		byte[] dataByte = null;
    	    
    		Base64 b64 = new Base64();
    		
    		if(b64.isBase64(val.toString())) {
    		
    			dataByte = b64.decodeBase64(val.toString()) ;
    	
    		}
    		else {
    			
    			System.out.println("string is not base 64\n");
    			
    		}
        	assert(dataByte != null);
        	Object obj = (MapReduceArraySerializer) SerializationUtils.deserialize(dataByte);
        	
        	if(obj instanceof MapReduceArraySerializer) {
        		MapReduceArraySerializer MAS = (MapReduceArraySerializer)obj;
        		System.out.println("Your object = " + Arrays.toString(MAS.numbers));
        		context.write(new Text("Average"), new Text(Arrays.toString(MAS.numbers)));
        	}
        	else {
        		System.out.println("Your object is not an instance of MapReduceArraySerializer\n");
        	}
       		
    	}
    }
    // use cleanup to do final sum average

    /*public void clean(Context context) {
    
    	int average = runningSum/numberOfDataPoints;
    	
    	try {
			context.write(new Text("Average"), new IntWritable(average));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
  
    	System.out.println("Average of data points is: " + average);
    	// write out json file

    }*/
}
