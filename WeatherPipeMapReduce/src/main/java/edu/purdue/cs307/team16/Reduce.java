package edu.purdue.cs307.team16;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {	

	public int runningSum;
	public int numberOfDataPoints = 0;
	
	
    public void reduce(Text keyname, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        
    	
    	// add data to runningSum and increment number of data points
    	// for the average
   
        for (IntWritable val : values) {
            runningSum += val.get();
            numberOfDataPoints++;
        }
        int average = runningSum/numberOfDataPoints;
        context.write(new Text("Average"), new IntWritable(average));

    }
    // use cleanup to do final sum average

    public void clean(Context context) {
    
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

    }
}
