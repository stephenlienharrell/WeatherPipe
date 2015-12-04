package edu.purdue.cs307.team16;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.PropertyConfigurator;

import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.FileWriter2;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;


public class ResearcherMapReduceAnalysis extends MapReduceAnalysis<double[], double[]> {
	
	public ResearcherMapReduceAnalysis(Configuration conf) {
		super(conf);
	}
	
	public ResearcherMapReduceAnalysis() {
		super(null);
	}
	
	protected double[] mapAnalyze(NetcdfFile nexradNetCDF) {
		int i;
		Array dataArray;
		double[] retArray = null;
		byte[] bytes;
		
		// SHAPE is scanR=7, radialR=360, gateR=1336



		Variable reflectivity = nexradNetCDF.findVariable("Reflectivity");
		if(reflectivity == null) return null;
		System.out.println("reflectivity shape - " + Arrays.toString(reflectivity.getShape()));
		Variable reflectivityHi = nexradNetCDF.findVariable("Reflectivity_HI");
		if(reflectivityHi == null) return null;
		System.out.println("reflectivityHi shape - " + Arrays.toString(reflectivityHi.getShape()));
		
		try {
			dataArray = reflectivity.read();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		if(dataArray.getShape()[0] != 7 || dataArray.getShape()[2] != 1336) return null;
		
		bytes = (byte[]) dataArray.copyTo1DJavaArray();
		
		retArray = new double[bytes.length];
		
		for(i = 0; i < bytes.length; i++) {
			retArray[i] = (double) bytes[i];
		}
				
		return retArray;
	
	}

	double[] runningSumsArray = null;
	int numberOfDataPoints = 0;
	
	protected double[] reduceAnalyze(double[] input) {
		if(input == null) return null;
		
		if(runningSumsArray == null) runningSumsArray = new double[input.length];
		double[] averageArray = new double[runningSumsArray.length];
		
		numberOfDataPoints++;

		for(int i = 0; i < input.length; i++) {
			runningSumsArray[i] += input[i];
			averageArray[i] = runningSumsArray[i]/numberOfDataPoints;
		}
		System.gc();
		return averageArray;
	}


	protected void outputFileWriter(double[] reduceOutput, String outputDir) {
		int i;
		
		// Instead of creating everything from scratch, load a netcdf file from 
		// the dataset we averaged that has the same shape and modify that.
		// file is available from: http://noaa-nexrad-level2.s3.amazonaws.com/2015/11/01/KIND/KIND20151101_000411_V06.gz
		String cheatFile = "KIND20151101_000411_V06";
		NetcdfFile ncfileIn = null; 
		try {
			ncfileIn = NetcdfFile.open(cheatFile);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		NetcdfFileWriter.Version version = NetcdfFileWriter.Version.netcdf3;
		String filenameOut = outputDir + "/netcdfOut";
		try {
			FileWriter2 writer2 = new ucar.nc2.FileWriter2(ncfileIn, filenameOut, version, null);
			NetcdfFile ncfileOut;

			ncfileOut = writer2.write();

			ncfileIn.close();
			ncfileOut.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// SHAPE is scanR=7, radialR=360, gateR=1336

		NetcdfFileWriter writer = null;
		
		try {
			writer = NetcdfFileWriter.openExisting(filenameOut);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		Variable reflectivity = writer.findVariable("Reflectivity");
		int[] shape = reflectivity.getShape();
		
		try {
			writer.setRedefineMode(true);
			writer.deleteVariableAttribute(reflectivity,  "reflectivity");
			writer.deleteVariableAttribute(reflectivity,  "reflectivity_HI");
			writer.setRedefineMode(false);

			writer.close();
			writer = NetcdfFileWriter.openExisting(filenameOut);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		
		byte[] byteOutput = new byte[reduceOutput.length];
		for(i=0; i<reduceOutput.length; i++) {
			byteOutput[i] = (byte)reduceOutput[i];
		}

		Array newReflectArray = Array.factory(byte.class, shape, byteOutput);
		try {


			writer.write(reflectivity, new int[3], newReflectArray);
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidRangeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
	}
	

	public static void main(String... aArgs){
		ResearcherMapReduceAnalysis thing = new ResearcherMapReduceAnalysis();
		thing.outputFileWriter(new double[7*360*1336], "x");
	}
	
}




/* 
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
 
 
 */

/*
 // example of scalar average
public class ResearcherMapReduceAnalysis extends MapReduceAnalysis<Double, Double> {
	
	public ResearcherMapReduceAnalysis(Configuration conf) {
		super(conf);
	}

	public ResearcherMapReduceAnalysis() {
		super(null);
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
