package edu.purdue.cs307.team16;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.json.JSONObject;

import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.NetcdfFile;

public class ResearcherMapReduceAnalysis extends MapReduceAnalysis<double[], double[]> {
	
	public ResearcherMapReduceAnalysis() {
		super();
	}
	
	protected double[] mapAnalyze(NetcdfFile nexradNetCDF) {
		Array dataArray;
		Index dataIndex;
		double[] retArray = null;
		
		// SHAPE is 3, 360, 324

		try {
			dataArray = nexradNetCDF.findVariable("Reflectivity").read();
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
		double[] averageArray = new double[input.length];
		
		if(runningSumsArray == null) runningSumsArray = new double[input.length];
	
		numberOfDataPoints++;

		for(int i = 0; i < input.length; i++) {
			runningSumsArray[i] += input[i];
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

/*
 // example of scalar average
public class ResearcherMapReduceAnalysis extends MapReduceAnalysis<Double, Double> {
	
	public ResearcherMapReduceAnalysis() {
		super();
	}
	
	protected Double mapAnalyze(NetcdfFile nexradNetCDF) {
		
		Array dataArray;
		Index dataIndex;
		byte dataByte = -1;
		
		// SHAPE is 3, 360, 324

		try {
			dataArray = nexradNetCDF.findVariable("Reflectivity").read();
	

			dataIndex = dataArray.getIndex();
			dataIndex.set(0,0,0);
			dataByte = dataArray.getByte(dataIndex);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println(ExceptionUtils.getStackTrace(e));
			System.exit(1);
		}
		
		return (double) dataByte;
	
	}
	
	public Double runningSum = 0.0;
	public int numberOfDataPoints = 0;
	Double average = 0.0;

	protected Double reduceAnalyze(Double input) {
		
		runningSum += input;
		numberOfDataPoints++;
		average = runningSum/numberOfDataPoints;
		return average;
	}
	

	protected void outputFileWriter(Double reduceOutput, String outputDir) {
		String outputFile = outputDir + "/jsonOutputFile";
		JSONObject jsonObj = new JSONObject();
		FileWriter fileWriter;
		
		jsonObj.put("Average", reduceOutput.toString());
		
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
*/
