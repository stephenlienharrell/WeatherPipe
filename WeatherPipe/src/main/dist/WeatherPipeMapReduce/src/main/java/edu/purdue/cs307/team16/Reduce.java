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
    	
    	String keyMessage = "**keyMessage:\n";
    	String valueMessage = "**valueMessage\n";
    	
    	for(Text val : str) {
    		byte[] dataByte = null;
    	    
  
    		Object obj =  null;
    		dataByte = Base64.decodeBase64(val.toString()) ;
    		obj = (MapReduceArraySerializer) SerializationUtils.deserialize(dataByte);
    		MapReduceArraySerializer MAS = (MapReduceArraySerializer)obj;
    		
    		context.write(new Text("Average"), new Text(Arrays.toString(MAS.numbers)));
    		
    		return;
/*
    		if(Base64.isBase64(val.toString())) {
    		
    			dataByte = Base64.decodeBase64(val.toString()) ;
    	
    		} else {
    //			context.write(new Text("not a base64 string"), new Text(val.toString()));
    			return;
    			
    		}
        	if(dataByte != null) {
        		obj = (MapReduceArraySerializer) SerializationUtils.deserialize(dataByte);
        	} else {
     //   		context.write(new Text("Error"), new Text("Object was null"));
        		return;
        	}
        	
        	if(obj instanceof MapReduceArraySerializer) {
        		MapReduceArraySerializer MAS = (MapReduceArraySerializer)obj;
        		System.out.println("Your object = " + Arrays.toString(MAS.numbers));
        		valueMessage = valueMessage + Arrays.toString(MAS.numbers) + "\n";
        		
        		
        	} else {
       // 		context.write(new Text("Error"), new Text("Your object is not an instance of MapReduceArraySerializer\n"));
        		return;
        	}
*/
    	}
   // 	context.write(new Text("Average"), new Text(valueMessage));

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
