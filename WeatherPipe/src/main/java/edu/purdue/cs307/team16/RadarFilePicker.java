package edu.purdue.cs307.team16;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import edu.purdue.cs307.team16.AWSInterface;


public class RadarFilePicker {
	

	public static ArrayList<String> getRadarFilesFromTimeRange(DateTime start, DateTime end, String station, AWSInterface awsInterface, String dataBucket){
	
		String lowBound = start.toString("yyyyMMdd_hhmmss");
		String uppBound = end.toString("yyyyMMdd_hhmmss");
		
		
		String key = "2010/01/01/";
		// need to generate list of buckets to list by enumerating
		// all days between the two times
		
		
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
		int stationLen = station.length();
		
		try {
			
			List<S3ObjectSummary> summaries = awsInterface.ListBucket(dataBucket, key);
			for (S3ObjectSummary objectSummary : summaries) {

				index = objectSummary.getKey().indexOf('.');
				if(objectSummary.getKey().substring(index+1, index+3).compareTo("gz") != 0)
					continue;	//skip the key with other format.
				
		//		System.out.println(objectSummary.getKey());
				index = objectSummary.getKey().indexOf('_');
				
				if(objectSummary.getKey().charAt(index-9-stationLen) != '/' || objectSummary.getKey().substring(index-8-stationLen, index-8).compareTo(station) != 0) {
					continue;	//if the station is not the one we need to find, just skip this object.
				}

				
				compDate = objectSummary.getKey().substring(index-8, index);	//the current object's date
				compTime = objectSummary.getKey().substring(index+1, index+7);	//the current object's time
				compInt1 = arr1[0].compareTo(compDate);
				compInt2 = arr2[0].compareTo(compDate);
				compInt3 = arr1[1].compareTo(compTime);
				compInt4 = arr2[1].compareTo(compTime);
				
				if(compInt1 <= 0 && compInt2 >= 0) {
					if(compInt3 <= 0 && compInt4 >= 0){
						format = objectSummary.getKey();
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

