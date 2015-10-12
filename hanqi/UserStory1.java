/*
 * Copyright 2010-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
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

/**
 * This sample demonstrates how to make basic requests to Amazon S3 using the
 * AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer
 * account, and be signed up to use Amazon S3. For more information on Amazon
 * S3, see http://aws.amazon.com/s3.
 * <p>
 * Fill in your AWS access credentials in the provided credentials file
 * template, and be sure to move the file to the default location
 * (/Users/Hanqi/.aws/credentials) where the sample code will load the credentials from.
 * <p>
 * <b>WARNING:</b> To avoid accidental leakage of your credentials, DO NOT keep
 * the credentials file in your source directory.
 *
 * http://aws.amazon.com/security-credentials
 */
public class UserStory1 {
	
    //Get input from the user
	
	public static String getInput() {
		String s = "";
		Scanner in = new Scanner(System.in);
		System.out.println("plese eneter a date range with time range in the format of yyyymmdd_tttttt-yyyymmdd_tttttt."
                           + " For example, 01:03:31 on the Jan.1st on 2010 should be presented as 20100101_010331");
		//check format correct?
		s = in.nextLine();
		return s;
	}
	
	/*get the beginning date&time and ending date&time from input
     and return a String array with size of 2
     */
	public static String[] getRange(String input) {
		//the period lower bound: array[0], upper bound: array[1].
		String[] array= new String[2];
		array = input.split("-");
		return array;
	}
	
	/*
	 * List all the objects in the bucket
	 */
	public static ArrayList<String> getFileNames(AmazonS3 s3, String[] array) {
		
		Region usWest2 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usWest2);
        
        String bucketName = "noaa-nexrad-level2";
        // String key = "1991/01/01/";
        
        ArrayList<String> ret = new ArrayList<String>();
        
        String[] arr1 = new String[2];	//arr1[0] = the date of the lower bound, [1] = the time of the lower bound
        String[] arr2 = new String[2];	//arr2[0] = the date of the upper bound, [1] = the time of the upper bound
        
        arr1 = array[0].split("_");
        arr2 = array[1].split("_");
        
        int index = -1;	//used to find the index of '-' in the file name.
        int compInt1 = 0;	//used to compare the date
        int compInt2 = 0;	//used to compare the date
        int compInt3 = 0;	//used to compare the time
        int compInt4 = 0;	//used to compare the time
        
        
        String compDate;	//substring(date) of a file name
        String compTime;	//substring(time) of a file name
        String format;		//used for a file name in a format we need.
        /*System.out.println("===========================================");
         System.out.println("Getting Started with Amazon S3");
         System.out.println("===========================================\n");*/
        
        try {
            
            /*
             * List objects in your bucket by prefix - There are many options for
             * listing the objects in your bucket.  Keep in mind that buckets with
             * many objects might truncate their results when listing their objects,
             * so be sure to check if the returned object listing is truncated, and
             * use the AmazonS3.listNextBatchOfObjects(...) operation to retrieve
             * additional results.
             */
            System.out.println("Listing objects");
            ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                                                         .withBucketName(bucketName));
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            	index = objectSummary.getKey().indexOf('.');
            	if(objectSummary.getKey().substring(index+1, index+3).compareTo("gz") != 0)
            		continue;	//skip the key with other format.
            	index = objectSummary.getKey().indexOf('_');
            	//if(index != -1) {
            	compDate = objectSummary.getKey().substring(index-8, index);
            	compTime = objectSummary.getKey().substring(index+1, index+7);
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
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                               + "a serious internal problem while trying to communicate with S3, "
                               + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
        return ret;
	}
	public static void main(String[] args) throws IOException {
		//String bucketName = "noaa-nexrad-level2";
		//String key = "2010/01/01/KABR/KABR20100101_010331_V03.gz";
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
		//AmazonS3 s3Client = new AmazonS3Client();
		AmazonS3 s3Client = new AmazonS3Client(credentials);
		
		
        String input = getInput();
        String[] array = getRange(input);
        ArrayList<String> list = getFileNames(s3Client, array);
        int size = list.size();
        for(int i = 0; i < size; i++) {
        	System.out.println(list.get(i));
        }
		
        //S3Object object = s3Client.getObject(
        //new GetObjectRequest(bucketName, key));
		//InputStream objectData = object.getObjectContent();
		// Process the objectData stream.
		//objectData.close();
		//System.out.println(objectData.read());
		//displayTextInputStream(objectData);
		
		//UncompressInputStream ucfile = new UncompressInputStream(objectData);
		//S3Sample s = new S3Sample();
    	//s.unzip(objectData, key1);
	}
	
	/*private static void displayTextInputStream(InputStream input) throws IOException {
     BufferedReader reader = new BufferedReader(new InputStreamReader(input));
     while (true) {
     String line = reader.readLine();
     if (line == null) break;
     
     System.out.println("    " + line);
     }
     System.out.println();
     }*/
	
    
}




