package edu.purdue.cs307.team16;

import java.io.IOException;
import java.io.Serializable;

import ucar.nc2.NetcdfFile;

public abstract class MapReduceAnalysis<MT, RT> {
	// MT Map return type
	// RT Reduce return type
	
	public MapReduceSerializer serializer;
		
	public MapReduceAnalysis() {

	}
	
	public void map(NetcdfFile nexradNetCDF) throws IOException {
		MT genericObject = mapAnalyze(nexradNetCDF);
		serializer = new MapReduceSerializer(genericObject);
	}	

	protected abstract MT mapAnalyze(NetcdfFile nexradNetCDF);
	
	public void reduce(Object input) throws IOException {
		if(input == null) throw new IOException("Input of analysis reduce is null");
		@SuppressWarnings("unchecked")
		RT genericObject = reduceAnalyze((MT) input);
		serializer = new MapReduceSerializer(genericObject);
	}	
	
	protected abstract RT reduceAnalyze(MT input);
	

}
