package edu.purdue.cs307.team16;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Days;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import edu.purdue.cs307.team16.AWSInterface;

import org.joda.time.Period;
public class RadarFilePicker {
	public static void addFile(ArrayList<String> ret, String lowBound, String uppBound, String station, String key, AWSInterface awsInterface, String dataBucket) {
		int index = -1;	//used to find the index of '-' in the file name.
		int compInt1 = 0;	//used to compare the date
		int compInt2 = 0;	//used to compare the date
		int compInt3 = 0;	//used to compare the time
		int compInt4 = 0;	//used to compare the time
		
		String[] arr1 = new String[2];	//arr1[0] = the date of the lower bound, [1] = the time of the lower bound
		String[] arr2 = new String[2];	//arr2[0] = the date of the upper bound, [1] = the time of the upper bound
		String compDate;	//substring(date) of a file name
		String compTime;	//substring(time) of a file name
		String format;		//used for a file name in a format we need.
		int stationLen = station.length();
		
		arr1 = lowBound.split("_");
		arr2 = uppBound.split("_");
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
	}

	public static ArrayList<String> getRadarFilesFromTimeRange(DateTime start, DateTime end, String station, AWSInterface awsInterface, String dataBucket){
		String lowBound = start.toString("yyyyMMdd_HHmmss");
		String uppBound = end.toString("yyyyMMdd_HHmmss");
		ArrayList<String> ret = new ArrayList<String>();
		int days;
		String key = null;
		Period everyday = Period.days(1);
		DateTime dt = start;
		String temp = null;
		//Date startForDiff = new Date(start.getYear(), start.getMonthOfYear(), start.getDayOfMonth());
		//Date endForDiff =  new Date(end.getYear(), end.getMonthOfYear(), end.getDayOfMonth());
		DateTime startForDiff = new DateTime(start.getYear(), start.getMonthOfYear(), start.getDayOfMonth(), 0, 0);
		DateTime endForDiff = new DateTime(end.getYear(), end.getMonthOfYear(), end.getDayOfMonth(), 0 ,0);
		//int days = Days.daysBetween(start, end).getDays();
		//days = Days.daysBetween(new DateTime(startForDiff), new DateTime(endForDiff)).getDays();
		days = Days.daysBetween(startForDiff, endForDiff).getDays();
		if(days == 0) {
			key = lowBound.substring(0, 4) + "/" + lowBound.substring(4, 6) + "/" + lowBound.substring(6, 8);
			addFile(ret, lowBound, uppBound, station, key, awsInterface, dataBucket);
		}
		else {
			System.out.println(days);
			for(int i = 0; i <= days; i++) {
				if(i == 0) {
					lowBound = start.toString("yyyyMMdd_HHmmss");
					uppBound = lowBound.substring(0, 9) + "235959";
					key = lowBound.substring(0, 4) + "/" + lowBound.substring(4, 6) + "/" + lowBound.substring(6, 8);
					addFile(ret, lowBound, uppBound, station, key, awsInterface, dataBucket);
				}
				else if(i == days){
					uppBound = end.toString("yyyyMMdd_HHmmss");
					lowBound = uppBound.substring(0, 9) + "000000";
					key = uppBound.substring(0, 4) + "/" + uppBound.substring(4, 6) + "/" + uppBound.substring(6, 8);
					addFile(ret, lowBound, uppBound, station, key, awsInterface, dataBucket);
				}
				else {
					dt = dt.plus(everyday);
					temp = dt.toString("yyyyMMdd_HHmmss");
					lowBound = temp.substring(0, 9) + "000001";
					uppBound = temp.substring(0, 9) + "235959";
					key = temp.substring(0, 4) + "/" + temp.substring(4, 6) + "/" + temp.substring(6, 8);
					addFile(ret, lowBound, uppBound, station, key, awsInterface, dataBucket);
					
				}
			}
		}
		return ret;
		
	}
	
	//this function checks if the flag of the station is 4 capital letters
	public static boolean checkStationType (String s) throws NullPointerException {
		try {
			if (s.isEmpty()){
				System.out.println("error: the string is empty");
				return false;
			}
			if (s.length() != 4) {
				System.out.println("error: the length of the string is not 4");
				return false;
			}
			for (int i = 0; i < s.length(); i++){
				if (s.charAt(i) > 90 || s.charAt(i) < 65){
					System.out.println("error: the string can only contain upper case letters");
					return false;
				}
			}
		}catch(NullPointerException e){
			System.out.println("error: the string is null");
			return false;
		}
		return true;
	}
	
}

