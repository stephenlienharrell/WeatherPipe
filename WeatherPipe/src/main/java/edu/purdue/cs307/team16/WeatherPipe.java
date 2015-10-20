package edu.purdue.cs307.team16;

import java.util.ArrayList;
import java.util.UUID;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import edu.purdue.cs307.team16.RadarFilePicker;

//import edu.purdue.cs307.team16.S3FileReadPrototype;
		 
public class WeatherPipe {

	public static void main(String[] args) {
		 final String dataBucket = "noaa-nexrad-level2";
		 final String dateFormatString = "dd/MM/yyyy HH:mm:ss";
		 final String dateDesc = "Date Format is " + dateFormatString;
		 final DateTimeFormatter dateFormat = DateTimeFormat.forPattern(
			dateFormatString);
		 DateTime startTime = null;
		 DateTime endTime = null;
		 ArrayList<String> radarFileNames;
		 final String jobID = UUID.randomUUID().toString();
		 AWSInterface awsInterface = new AWSInterface(jobID); 
		 String jobHadoopJarURL, jobInputURL;
		 String hadoopJarFileName = "WeatherPipeMapReduce.jar"; // figure out how to automate this
		 String instanceType = "c3.xlarge"; //Make this a flag
		 int instanceCount = 1; // Make this a flag
		 String bucketName;
		 
		 // create Options object
		 Options options = new Options();
		 CommandLineParser parser = new DefaultParser();
		 
	     // add options for jar file and radar station if time is available
		 String station = "KAKQ";
		 options.addOption("s", "start_time", true, "Start time of analysis. " + dateDesc);
		 options.addOption("e", "end_time", true, "End time of analysis. " + dateDesc);
		 
		 try {
			 // parse the command line arguments
			 CommandLine line = parser.parse( options, args );
			 
			 //System.out.println(line.getOptionValue("start_time") + " " + line.getOptionValue("end_time"));
		 
			 if( line.hasOption( "start_time" ) &&
					 (line.getOptionValue("start_time") != null) ) {
				startTime = DateTime.parse(
					line.getOptionValue("start_time"), 
					dateFormat);
			 } else {
				System.out.println("Flag start_time is required");
				System.exit(1);
			 } 
				
			 if( line.hasOption( "end_time" ) ) {
				endTime = DateTime.parse(
					line.getOptionValue("end_time"), 
					dateFormat);
			 } else {
				System.out.println("Flag end_time is required");
				System.exit(1);
			 } 
		 } catch( ParseException exp ) {
			 System.out.println( "Unexpected exception:" + exp.getMessage() );
		 }
		 
		 
		 System.out.println("Searching NEXRAD Files");
		 radarFileNames = RadarFilePicker.getRadarFilesFromTimeRange(startTime, endTime, station, awsInterface, dataBucket);
		 System.out.println("Found " + radarFileNames.size() + " NEXRAD Radar files between " + startTime.toString() + " and " + endTime.toString() );
		 
		 System.out.println("Search for/Create WeatherPipe S3 bucket");
		 bucketName = awsInterface.FindOrCreateWeatherPipeJobBucket();
		 if(bucketName == null) {
			 System.out.println("Bucket was not created correctly");
			 System.exit(1);
		 }
		 System.out.println("Using bucket " + bucketName);
		 
		 System.out.print("Uploading Input file... ");		 
		 jobInputURL = awsInterface.UploadInputFileList(radarFileNames, dataBucket);
		 System.out.println("Complete");
		 
		 System.out.print("Uploading Jar file... ");
		 jobHadoopJarURL = awsInterface.UploadMPJarFile(hadoopJarFileName);
		 System.out.println("Complete");
		 
		 awsInterface.CreateEMRJob(jobInputURL, jobHadoopJarURL, instanceCount, instanceType);
	
	}
}
