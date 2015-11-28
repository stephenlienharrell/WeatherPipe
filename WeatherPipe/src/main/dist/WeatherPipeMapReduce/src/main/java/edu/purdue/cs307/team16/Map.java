package edu.purdue.cs307.team16;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import ucar.nc2.NetcdfFile;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.output.ByteArrayOutputStream;

/*
    Input is of the form:
    ("bucketname", "key")
    ("bucketname", "key")
    ("bucketname", "key")

    At this point, I assume they are in the same bucket. 
*/


public class Map extends Mapper<LongWritable, Text, Text, Text> {
	
    private Text data = new Text();
    private Text word = new Text();
    AmazonS3 s3 = null;
    

    public void map(LongWritable l, Text keyname, Context context) throws IOException, InterruptedException {
    	
    	if(s3 == null) {
    		ClientConfiguration conf = new ClientConfiguration();
    		// 2 minute timeout

    		s3 = new AmazonS3Client(new AnonymousAWSCredentials(), conf);
    		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
    		s3.setRegion(usEast1);
    	}
		
		S3Object object;
		byte[] buf = new byte[1024];
		int len;

		GZIPInputStream gunzip;
		ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
		ByteArrayOutputStream orginalByteArrayStream = new ByteArrayOutputStream();
		S3ObjectInputStream objectInputStream;
		Level level;
		Logger logger;
		NetcdfFile ncfile = null;

		ResearcherMapReduceAnalysis analysis = null;
		

		//log4j stuff
		BasicConfigurator.configure();
		level = Level.OFF;
		logger = org.apache.log4j.Logger.getRootLogger();
		logger.setLevel(level);
		
		// Setting bucket parameters
		String bucketAndKey = keyname.toString();
	    String bucketName = bucketAndKey.split(" ")[0];
        String key = bucketAndKey.split(" ")[1];
       
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

				
		}
		
		catch (IOException ioe) {
			System.out.println(ExceptionUtils.getStackTrace(ioe));
			ncfile = null;
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
		analysis = new ResearcherMapReduceAnalysis();
		if (!analysis.map(ncfile)) {
			analysis.serializer.serializeMe = null;
		} else {
			ncfile.close();
		}
		byteArrayStream.close();
           
        	
       	byte[] databyte = SerializationUtils.serialize(analysis.serializer);      	
       	String byte_to_string = Base64.encodeBase64String(databyte);

       	word.set("Mapperid: " + String.valueOf(context.getTaskAttemptID().getTaskID().getId()));
       	data.set(byte_to_string);   	
       	context.write(word, data);

    }
}
