package edu.purdue.eaps.weatherpipe.weatherpipemapreduce;


import org.apache.hadoop.conf.Configuration;

import ucar.nc2.NetcdfFile;



public class ResearcherMapReduceAnalysis extends MapReduceAnalysis<Integer, Double> {
	
	public ResearcherMapReduceAnalysis(Configuration conf) {
		super(conf);
	}
	
	public ResearcherMapReduceAnalysis() {
		super(null);
	}
	
	// This is an empty example with the bare minimum of what is needed
	// Please change the types as needed. You need to change the types
	// in the class header. The first one is the output of mapAnalyze and
	// the input of reduceAnalyze. The second one is the output of reduceAnalyze
	// and the input of outputFileWriter. They do not have to be different types
	// They have different to illustrate what goes where. 
	// The types must be serializable: http://www.tutorialspoint.com/java/java_serialization.htm
	
	protected Integer mapAnalyze(NetcdfFile nexradNetCDF) {

		return new Integer(0);
	}


	
	protected Double reduceAnalyze(Integer input) {
		return new Double(0);
		
	}


	protected void outputFileWriter(Double reduceOutput, String outputDir) {
	
	}
}
