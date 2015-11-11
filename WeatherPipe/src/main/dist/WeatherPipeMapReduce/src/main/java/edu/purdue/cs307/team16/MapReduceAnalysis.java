package edu.purdue.cs307.team16;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;

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
	
	public void reduce(String input) throws IOException {
		byte[] dataByte;
		MapReduceSerializer obj;
		dataByte = Base64.decodeBase64(input);
		obj = (MapReduceSerializer) SerializationUtils.deserialize(dataByte);
		if(input == null) throw new IOException("Input of analysis reduce is null");
		@SuppressWarnings("unchecked")
		RT genericObject = reduceAnalyze((MT) obj.serializeMe);
		serializer = new MapReduceSerializer(genericObject);
	}	
	
	protected abstract RT reduceAnalyze(MT input);
	
	@SuppressWarnings("unchecked")
	public void writeFile(String input, String outputDir) throws IOException {
		byte[] dataByte;
		MapReduceSerializer obj;
		dataByte = Base64.decodeBase64(input);
		obj = (MapReduceSerializer) SerializationUtils.deserialize(dataByte);
		outputFileWriter((RT) obj.serializeMe, outputDir);
	}

	protected abstract void outputFileWriter(RT input, String outputDir);
		



}
