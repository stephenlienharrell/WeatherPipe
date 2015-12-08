package edu.purdue.eaps.weatherpipe;

import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class AWSAnonInterface {


	public String jobDirName;

	
	public String jobOutput;

	
	AmazonS3 s3AnonClient;
	
	public AWSAnonInterface() {
		String weatherPipeBinaryPath = WeatherPipe.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		String log4jConfPath = weatherPipeBinaryPath.substring(0, weatherPipeBinaryPath.lastIndexOf("/")) + "/log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);
		AWSCredentials creds = new AnonymousAWSCredentials();
		s3AnonClient = new AmazonS3Client(creds);  
	}

	
	public List<S3ObjectSummary> ListBucket(String bucketName, String key) {
		
		ObjectListing listing = s3AnonClient.listObjects( bucketName, key );
		List<S3ObjectSummary> summaries = listing.getObjectSummaries();

		while (listing.isTruncated()) {
		   listing = s3AnonClient.listNextBatchOfObjects(listing);
		   summaries.addAll(listing.getObjectSummaries());
		}
		
		return summaries;
	}

}
