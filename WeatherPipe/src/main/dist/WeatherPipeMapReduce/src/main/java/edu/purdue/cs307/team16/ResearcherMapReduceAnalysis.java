package edu.purdue.cs307.team16;

import java.io.IOException;

import org.apache.commons.lang3.exception.ExceptionUtils;

import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.NetcdfFile;

public class ResearcherMapReduceAnalysis extends MapReduceAnalysis<Double, Double> {
	
	public ResearcherMapReduceAnalysis() {
		super();
	}
	
	
	public Double mapAnalyze(NetcdfFile nexradNetCDF) {
		
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

}
