package edu.purdue.eaps.weatherpipe.weatherpipemapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.json.JSONObject;

import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;


 // Example of array average
 
 public class ResearcherMapReduceAnalysis extends MapReduceAnalysis<double[], double[]> {
	
	public ResearcherMapReduceAnalysis(Configuration conf) {
		super(conf);
	}
	
	public ResearcherMapReduceAnalysis() {
		super(null);
	}
	
	protected double[] mapAnalyze(NetcdfFile nexradNetCDF) {
		Array dataArray;
		Index dataIndex;
		double[] retArray = null;
		
		// SHAPE is 3, 360, 324

		try {
			Variable netcdf = nexradNetCDF.findVariable("Reflectivity");
			if(netcdf == null) return null;
			dataArray = netcdf.read();
	    	dataIndex = dataArray.getIndex();
	    	// get shape
	    	int[] shape = dataArray.getShape();	
	    	
	    	retArray = new double[shape[2]];
	    	for(int i = 0; i < shape[2]; i++) {
	        	
	        	retArray[i] = dataArray.getDouble(dataIndex.set(0,0,i));

	    	}
	    	
			System.out.println("Array from map: " + Arrays.toString(retArray));
	    	
	    	
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println(ExceptionUtils.getStackTrace(e));
			System.exit(1);
		}
		
		return retArray;
	
	}

	double[] runningSumsArray = null;
	int numberOfDataPoints = 0;
	
	protected double[] reduceAnalyze(double[] input) {
		int length;
		if(input == null && numberOfDataPoints == 0) return new double[1];
		
		if(runningSumsArray == null) runningSumsArray = new double[input.length];
		double[] averageArray = new double[runningSumsArray.length];
		
		if(input != null) numberOfDataPoints++;
		
		if(input == null || input.length > runningSumsArray.length) length = runningSumsArray.length;
		else length = input.length;
		
		
		

		for(int i = 0; i < length; i++) {
			if(input != null) runningSumsArray[i] += input[i];
			averageArray[i] = runningSumsArray[i]/numberOfDataPoints;
		}

		return averageArray;
	}


	protected void outputFileWriter(double[] reduceOutput, String outputDir) {
		String outputFile = outputDir + "/jsonOutputFile";
		JSONObject jsonObj = new JSONObject();
		FileWriter fileWriter;
		
		jsonObj.put("Average Array", Arrays.toString(reduceOutput));
		
		try {
			fileWriter = new FileWriter(outputFile);
			fileWriter.write(jsonObj.toString() + "\n");
			fileWriter.flush();
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}

}
 