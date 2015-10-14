package edu.purdue.cs307.team16;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;

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


// goal: to be able to read data from s3

public class S3_test1 {
	
	public static void main(String[] args) {



		AmazonS3 s3 = new AmazonS3Client(new AnonymousAWSCredentials());
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		s3.setRegion(usEast1);
		S3Object object;
		byte[] buf = new byte[1024];
		int len;
		GZIPInputStream gunzip;
		ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();

		// Setting bucket parameters
		String bucketName = "noaa-nexrad-level2";
		String key = "2010/01/01/KDDC/KDDC20100101_073731_V03.gz";
		
		//this format is for windows, please chanege this in your own machine
		try {
			// Download required object from S3
			System.out.println("Downloading an object");
			//S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
			object = s3.getObject(bucketName, key);
			gunzip = new GZIPInputStream(object.getObjectContent());

			while((len = gunzip.read(buf)) != -1){
				byteArrayStream.write(buf, 0, len);
			}
			
			
			
			System.out.println("The object is downloaded successfully!");
			try {
				NetcdfFile ncfile = NetcdfFile.openInMemory(key, byteArrayStream.toByteArray());

				//System.out.println("Description: " + ncfile.getFileTypeDescription());
				//System.out.println("Cache Name: " + ncfile.getCacheName());
				//System.out.println("Demension: " + ncfile.getDimensions());
				
				//Get the shape: length of Variable in each dimension.
				for(int i = 0; i < ncfile.findVariable("Reflectivity").getRank();i++)
				System.out.println("Reflectivity" + i + ": " + ncfile.findVariable("Reflectivity").getShape()[i]);
				
				System.out.println(ncfile.findVariable("Reflectivity").getDataType().toString());
				Array data = ncfile.findVariable("Reflectivity").read();
				Index dataIndex = data.getIndex();
				dataIndex.set(0,0,0);
				byte dataByte = data.getByte(dataIndex);
//				int i = 0;
//				while(data.hasNext()) {
//					System.out.println(data.next() + " " + i);
//					i++;
//				}
				System.out.println(dataByte);
				
				/*
				Variable v = ncfile.findVariable("Reflectivity");
				System.out.println(v.getSize());
				Array data = v.read();
				//assert data.getSize() == 360720;
				NCdumpW.printArray(data);
				*/
				ncfile.close();

			    
				//System.out.println("Reflectivity: " + ncfile.getGlobalAttributes());
				//System.out.println("Detial Information: " + ncfile.getDetailInfo());
				//System.out.println("Variables: " + ncfile.getVariables());
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
	}
}
