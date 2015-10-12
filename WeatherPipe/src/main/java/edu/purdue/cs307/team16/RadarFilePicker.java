package edu.purdue.cs307.team16;

import java.awt.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.io.*;

import org.joda.time.DateTime;

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
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;


public class RadarFilePicker {
	
	//Get input from the user

	// this should move to AWS library later
	public static AmazonS3 getS3() {
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
											"Cannot load the credentials from the credential profiles file. " +
											"Please make sure that your credentials file is at the correct " +
											"location (/Users/Hanqi/.aws/credentials), and is in valid format.",
											e);
		}
		return new AmazonS3Client(credentials);
	}

	public static ArrayList<String> getRadarFilesFromTimeRange(DateTime start, DateTime end, AmazonS3 s3){
		String lowBound = start.toString("yyyyMMdd_hhmmss");
		String uppBound = end.toString("yyyyMMdd_hhmmss");
		Region awsRegion = Region.getRegion(Regions.US_EAST_1);
		s3.setRegion(awsRegion);
		
		String bucketName = "noaa-nexrad-level2";
		// String key = "1991/01/01/";
		
		ArrayList<String> ret = new ArrayList<String>();
		String[] arr1 = new String[2];	//arr1[0] = the date of the lower bound, [1] = the time of the lower bound
		String[] arr2 = new String[2];	//arr2[0] = the date of the upper bound, [1] = the time of the upper bound
		
		arr1 = lowBound.split("_");
		arr2 = uppBound.split("_");
		
		int index = -1;	//used to find the index of '-' in the file name.
		int compInt1 = 0;	//used to compare the date
		int compInt2 = 0;	//used to compare the date
		int compInt3 = 0;	//used to compare the time
		int compInt4 = 0;	//used to compare the time
		
		
		String compDate;	//substring(date) of a file name
		String compTime;	//substring(time) of a file name
		String format;		//used for a file name in a format we need.
		
		try {
			
			ObjectListing objectListing = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
				index = objectSummary.getKey().indexOf('.');
				if(objectSummary.getKey().substring(index+1, index+3).compareTo("gz") != 0)
					continue;	//skip the key with other format.
				
				index = objectSummary.getKey().indexOf('_');
				
				compDate = objectSummary.getKey().substring(index-8, index);	//the current object's date
				compTime = objectSummary.getKey().substring(index+1, index+7);	//the current object's time
				compInt1 = arr1[0].compareTo(compDate);
				compInt2 = arr2[0].compareTo(compDate);
				compInt3 = arr1[1].compareTo(compTime);
				compInt4 = arr2[1].compareTo(compTime);
				
				if(compInt1 <= 0 && compInt2 >= 0) {
					if(compInt3 <= 0 && compInt4 >= 0){
						format = "(" + bucketName + ", " + objectSummary.getKey() + ")";
						ret.add(format);
					}
					//(bucketname, key)
				}
				//}
			}
			
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
							   + "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:	" + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:	   " + ase.getErrorType());
			System.out.println("Request ID:	   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
							   + "a serious internal problem while trying to communicate with S3, "
							   + "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
		return ret;
		
	}
	
	
}

