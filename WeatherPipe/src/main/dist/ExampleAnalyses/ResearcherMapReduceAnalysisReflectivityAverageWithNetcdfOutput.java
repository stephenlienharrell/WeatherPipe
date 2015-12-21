package edu.purdue.eaps.weatherpipe.weatherpipemapreduce;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import ucar.ma2.Array;
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
		
		try {
			dataArray = reflectivity.read();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		
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
		if(runningSumsArray != null && input.length != runningSumsArray.length) return null;
		
		if(runningSumsArray == null) runningSumsArray = new double[input.length];
		double[] averageArray = new double[runningSumsArray.length];
		
		numberOfDataPoints++;

		for(int i = 0; i < input.length; i++) {
			runningSumsArray[i] += input[i];
			averageArray[i] = runningSumsArray[i]/numberOfDataPoints;
		}
		
		
		return averageArray;
	}


	protected void outputFileWriter(double[] reduceOutput, String outputDir) {
		int i;
		NetcdfFile ncfileIn = null; 
		Boolean shapeFound = false;
		String line = null;
		String fileList = outputDir + "/job_setup/" + outputDir.split("WeatherPipeJob")[1] + "_input";
		BufferedReader lineRead = null;
		Variable reflectivity2 = null;
		int[] shape = null;
		byte[] byteOutput;
		
		// Instead of creating everything from scratch, load a netcdf file from 
		// the dataset we averaged that has the same shape and modify that.
		try {
			lineRead = new BufferedReader(new FileReader(fileList));
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
			System.exit(1);
		}
		while(!shapeFound) {
			try {
				if((line = lineRead.readLine()) == null) {
					System.out.println("Unable to find file with the same shape");
					System.exit(1);
					
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			ncfileIn = loadNCFileFromS3(line.split(" ")[0], line.split(" ")[1]);
			if(ncfileIn == null) continue;
			reflectivity2 = ncfileIn.findVariable("Reflectivity");
			if(reflectivity2 == null) continue;
			try {
				shape = reflectivity2.read().getShape();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(shape[0]*shape[1]*shape[2] == reduceOutput.length) shapeFound = true;
		}
		if(!shapeFound) {
			System.out.println("Did not find matching file to train input");
			System.exit(1);
		}
		
		// Convert this to something we can write/rewrite
		
		NetcdfFileWriter.Version version = NetcdfFileWriter.Version.netcdf3;
		String filenameOut = outputDir + "/netcdfOut";
		
		try {
			FileWriter2 writer2 = new ucar.nc2.FileWriter2(ncfileIn, filenameOut, version, null);
			NetcdfFile ncfileOut;

			ncfileOut = writer2.write();
			ncfileOut.findVariable("Reflectivity");
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
		shape = reflectivity.getShape();

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


		byteOutput = new byte[reduceOutput.length];
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
	
	NetcdfFile loadNCFileFromS3(String bucketName, String key) {

    	ClientConfiguration conf = new ClientConfiguration();
    	// 2 minute timeout

    	AmazonS3Client s3 = new AmazonS3Client(new AnonymousAWSCredentials(), conf);
    	Region usEast1 = Region.getRegion(Regions.US_EAST_1);
    	s3.setRegion(usEast1);
    	
 
		
		S3Object object;
		byte[] buf = new byte[1024];
		int len;

		GZIPInputStream gunzip;
		@SuppressWarnings("resource")
		ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
		ByteArrayOutputStream orginalByteArrayStream = new ByteArrayOutputStream();
		S3ObjectInputStream objectInputStream;
		Level level;
		Logger logger;
		NetcdfFile ncfile = null;

		//log4j stuff
		BasicConfigurator.configure();
		level = Level.OFF;
		logger = org.apache.log4j.Logger.getRootLogger();
		logger.setLevel(level);
       
		try {
			
			object = s3.getObject(bucketName, key);
			objectInputStream = object.getObjectContent();
			
			while((len = objectInputStream.read(buf)) != -1){
				orginalByteArrayStream.write(buf, 0, len);
			}
			
			gunzip = new GZIPInputStream(new ByteArrayInputStream(orginalByteArrayStream.toByteArray()));
			
			orginalByteArrayStream.close();

			while((len = gunzip.read(buf)) != -1){
				byteArrayStream.write(buf, 0, len);
				
			}
			
			ncfile = NetcdfFile.openInMemory(key, byteArrayStream.toByteArray());

				
		} catch (IOException|IllegalStateException  ioe) {
			System.out.println("Data file " + key.toString() + "was unable to be loaded.");
			System.out.println(ExceptionUtils.getStackTrace(ioe));
			return null;
		}

		
		catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} 
		catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
		return ncfile;
		
	}
	

	public static void main(String... aArgs){
		// for testing
		
		ResearcherMapReduceAnalysis thing = new ResearcherMapReduceAnalysis();
		thing.outputFileWriter(new double[7*360*1336], "x");
	}
	
}