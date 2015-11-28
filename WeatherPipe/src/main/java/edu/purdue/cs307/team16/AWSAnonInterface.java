package edu.purdue.cs307.team16;

import java.util.List;

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
