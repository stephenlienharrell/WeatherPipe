package edu.purdue.cs307.team16;

import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.ArrayList;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import edu.purdue.cs307.team16.RadarFilePicker;

public class WeatherPipe {
	// final String dataBucket = "noaa-nexrad-level2";
	final static String dateFormatString = "dd/MM/yyyy HH:mm:ss";
	final static String dateDesc = "Date Format is " + dateFormatString;
	final static DateTimeFormatter dateFormat = DateTimeFormat.forPattern(dateFormatString);
	static String dataBucket = "noaa-nexrad-level2";
	public static DateTime startTime = null;
	public static DateTime endTime = null;
	static ArrayList<String> radarFileNames;
	public static String jobID = null;
	static AWSInterface awsInterface = null;
	static String jobHadoopJarURL, jobInputURL;
	public static String instanceType = null; 
	public static int instanceCount; 
	public static String bucketName = null;
	public static String station = null;
	public static WeatherPipeFileWriter fileWriter = new  WeatherPipeFileWriter();

	public static void main(String[] args) {

		MapReduceBuilder builder = new MapReduceBuilder(null);
		
		addFlags(args);		

		String mapReduceJarLocation = builder.buildMapReduceJar();
		

		System.out.println("Searching NEXRAD Files");
		radarFileNames = RadarFilePicker.getRadarFilesFromTimeRange(startTime, endTime, station, awsInterface,
				dataBucket);
		System.out.println("Found " + radarFileNames.size() + " NEXRAD Radar files between " + startTime.toString()
				+ " and " + endTime.toString());
		System.out.println();
		System.out.println("Search for/Create WeatherPipe S3 bucket");
		bucketName = awsInterface.FindOrCreateWeatherPipeJobBucket();
		if (bucketName == null) {
			System.out.println("Bucket was not created correctly");
			System.exit(1);
		}
		System.out.println("Using bucket " + bucketName);

		System.out.print("Uploading Input file... ");
		jobInputURL = awsInterface.UploadInputFileList(radarFileNames, dataBucket);
		System.out.println("Complete");

		System.out.print("Uploading Jar file... ");
		jobHadoopJarURL = awsInterface.UploadMPJarFile(mapReduceJarLocation);
		System.out.println("Complete");

		awsInterface.CreateEMRJob(jobInputURL, jobHadoopJarURL, instanceCount, instanceType);

		try {
			fileWriter.writeOutput(awsInterface.jobOutput, awsInterface.jobDirName, mapReduceJarLocation);
		} catch (MalformedURLException | ClassNotFoundException | NoSuchMethodException | SecurityException
				| InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		awsInterface.close();
	}

	public static void addFlags(String[] args) {
		// create Options object
		Options options = new Options();
		CommandLineParser parser = new DefaultParser();

		// add options for jar file and radar station if time is available

		options.addOption("b", "bucket_name", true,
				"Bucket name of analysis. " + "the buckent name looks like \"noaa-nexrad-level2\". ");
		options.addOption("s", "start_time", true, "Start time of analysis. " + dateDesc);
		options.addOption("e", "end_time", true, "End time of analysis. " + dateDesc);
		options.addOption("st", "station", true,
				"station of analysis. " + "The name of station format is 4 capital letters. ");
		options.addOption("id", "jobID", true, "jobID of analysis. ");
		options.addOption("i_T", "instanceType", true,
				"instanceType of analysis. The instanceType looks like \"c3.xlarge\". ");
		options.addOption("i_C", "instanceCount", true, "instanceCount of analysis. ");

		try {
			// parse the command line arguments
			CommandLine line = parser.parse(options, args);

			if (line.hasOption("start_time") && (line.getOptionValue("start_time") != null)) {
				startTime = DateTime.parse(line.getOptionValue("start_time"), dateFormat);
			} else {
				System.out.println("Flag start_time is required");
				System.exit(1);
			}

			if (line.hasOption("end_time")) {
				endTime = DateTime.parse(line.getOptionValue("end_time"), dateFormat);
			} else {
				System.out.println("Flag end_time is required");
				System.exit(1);
			}

			if (line.hasOption("station") && RadarFilePicker.checkStationType(line.getOptionValue("station"))) {
				station = line.getOptionValue("station");
			} else {
				System.out.println("Flag station is required");
				System.exit(1);
			}

			if (line.hasOption("jobID")) {
				jobID = line.getOptionValue("jobID");
			}

			if (line.hasOption("bucket_name")) {
				bucketName = line.getOptionValue("bucket_name");
				awsInterface = new AWSInterface(jobID, bucketName);

			} else {
				awsInterface = new AWSInterface(jobID);

			}

			if (line.hasOption("instanceType")) {
				instanceType = line.getOptionValue("instanceType");
			}

			if (line.hasOption("instanceCount")) {
				instanceCount = Integer.parseInt(line.getOptionValue("instanceCount"));
			} else {
				instanceCount = 1;
			}

		} catch (ParseException exp) {
			System.out.println("Unexpected exception:" + exp.getMessage());
			System.exit(1);
		}

	}
}
