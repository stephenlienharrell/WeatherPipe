import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;

import ucar.ma2.Array;
import ucar.nc2.NCdumpW;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.util.cache.FileCacheable;

// goal: to be able to read data from s3

public class S3_test1 {
	
	public static void main(String[] args) {

		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} 
		catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"Please make sure that your credentials file is at the correct " +
							"location (/Users/lalavaishnode/.aws/credentials), and is in valid format.",
							e);
			
		}

		AmazonS3 s3 = new AmazonS3Client(credentials);
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
