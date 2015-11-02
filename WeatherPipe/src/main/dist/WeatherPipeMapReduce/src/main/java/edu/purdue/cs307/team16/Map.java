package edu.purdue.cs307.team16;
import java.io.Serializable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;


import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.NetcdfFile;

import edu.purdue.cs307.team16.MapReduceArraySerializer;

import org.apache.commons.codec.binary.BaseNCodec;
import org.apache.commons.codec.binary.Base64;

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
    

    public void map(LongWritable l, Text keyname, Context context) throws IOException, InterruptedException {
    	
		AmazonS3 s3 = new AmazonS3Client(new AnonymousAWSCredentials());
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		s3.setRegion(usEast1);
		S3Object object;
		byte[] buf = new byte[1024];
		int len;
		Array dataArray;
		Index dataIndex;
		byte dataByte = -1;
		GZIPInputStream gunzip;
		ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
		Level level;
		Logger logger;
		

		//log4j stuff
		BasicConfigurator.configure();
		level = Level.OFF;
		logger = org.apache.log4j.Logger.getRootLogger();
		logger.setLevel(level);
		
		// Setting bucket parameters
		String bucketAndKey = keyname.toString();
	    String bucketName = bucketAndKey.split(" ")[0];
        String key = bucketAndKey.split(" ")[1];
       
        // String bucketName = "noaa-nexrad-level2";
		//String key = "2010/01/01/KDDC/KDDC20100101_073731_V03.gz";
		
		//this format is for windows, please chanege this in your own machine
		/*try {
			
			
			// Download required object from S3
			// System.out.println("Downloading an object");
			//S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
			
			object = s3.getObject(bucketName, key);
			gunzip = new GZIPInputStream(object.getObjectContent());

			while((len = gunzip.read(buf)) != -1){
				byteArrayStream.write(buf, 0, len);
			}
			
			// System.out.println("The object is downloaded successfully!");
			try {
				NetcdfFile ncfile = NetcdfFile.openInMemory(key, byteArrayStream.toByteArray());
				
			
				
				// SHAPE is 3, 360, 324
				dataArray = ncfile.findVariable("Reflectivity").read();
				dataIndex = dataArray.getIndex();
				dataIndex.set(0,0,0);
				dataByte = dataArray.getByte(dataIndex);
				ncfile.close();

		
				
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
		catch (IOException ioe) {
			System.out.println(ioe);
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
        */
        
        try {
        	
        	/*
            word.set(key);
            assert(dataByte >= 0);
            data.set(dataByte);
            context.write(word, data);
            */
        	
        	int[] myIntArray = new int[]{1,2,3};
        	MapReduceArraySerializer l1 = new MapReduceArraySerializer(myIntArray);
        	byte[] databyte = SerializationUtils.serialize(l1);
        	
        	Base64 b64 = new Base64();
        	String byte_to_string = b64.encodeBase64String(databyte);
        	
        	
        	System.out.println("byte_to_string = " + byte_to_string + "\n");
          	assert(byte_to_string != null);
        	
        	
        	word.set(key);
        	data.set(byte_to_string);
        	
        	context.write(word, data);
        	
        }
        catch (Exception e) {
            // Send signal

        }
    }
}
