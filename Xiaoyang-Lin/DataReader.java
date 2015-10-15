import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
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

// goal: to be able to read data from s3

public class DataReader {
	
	String bucketName;
	String key;
	String filename;


	public DataReader(String bucketName,String key, String filename) {
		this.bucketName = bucketName;
		this.key = key;
		this.filename = filename;
	}
	
	public static void main(String[] args) {
		String bucketName = "noaa-nexrad-level2";
		String key = "2010/01/01/KDDC/KDDC20100101_073731_V03.gz";

		
		NetcdfFile ncfile = download(bucketName, key);
		printVariable(ncfile, "Reflectivity");
		//ncfile.close();
	}
	
	//Print the shape: length of Variable in each dimension.
	public static void printVariable(NetcdfFile ncfile, String v){
		for(int i = 0; i < ncfile.findVariable(v).getRank();i++)
			System.out.println(v + i + ": " + ncfile.findVariable(v).getShape()[i]);
	}
	
	//the function to download the file and uncompress it into Netcdf format
	public static NetcdfFile download(String bucketName,String key) {
		AWSCredentials credentials = null;
		NetcdfFile ncfile = null;
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
		
		try {
			//Download required object from S3
			System.out.println("Downloading an object");
			/*S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
			s3.getObject(
			        new GetObjectRequest(bucketName, key),
			        new File(filename)
			);
			*/
			object = s3.getObject(bucketName, key);
			gunzip = new GZIPInputStream(object.getObjectContent());
			
			while((len = gunzip.read(buf)) != -1){
				byteArrayStream.write(buf, 0, len);
			}
			System.out.println("The object is downloaded successfully!");
		
			try {
				ncfile = NetcdfFile.openInMemory(key, byteArrayStream.toByteArray());
				//ncfile = NetcdfFile.open(filename);
	
				//System.out.println("Description: " + ncfile.getFileTypeDescription());
				//System.out.println("Cache Name: " + ncfile.getCacheName());
				//System.out.println("Demension: " + ncfile.getDimensions());
				
				//Get the shape: length of Variable in each dimension.

				/*
				Variable v = ncfile.findVariable("Reflectivity");
				System.out.println(v.getSize());
				Array data = v.read();
				//assert data.getSize() == 360720;
				NCdumpW.printArray(data);
				*/

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
		return ncfile;
	}
	
	
	//useless functions
	public void setBucket(String bucketName) {
		this.bucketName = bucketName;
	}
	
	public void setKey(String key) {
		this.key = key;
	}
	
	public void setFileName(String filename) {
		this.filename = filename;
	}
	
	public String getBucket() {
		return bucketName;
	}
	
	public String getKey() {
		return key;
	}
	
	public String filename() {
		return filename;
	}
}
