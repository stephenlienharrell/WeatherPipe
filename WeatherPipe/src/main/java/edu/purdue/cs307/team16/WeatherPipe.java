package edu.purdue.cs307.team16;

import java.io.IOException;
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
	final static String dateFormatString = "dd/MM/yyyy HH:mm:ss";
	final static String dateDesc = "Date Format is " + dateFormatString;
	final static DateTimeFormatter dateFormat = DateTimeFormat.forPattern(dateFormatString);
	static String dataBucket = "noaa-nexrad-level2";
	public static DateTime startTime = null;
	public static DateTime endTime = null;
	static ArrayList<String> radarFileNames;
	public static String jobID = null;
	public static AWSInterface awsInterface = null;
	public static AWSAnonInterface awsAnonInterface = new AWSAnonInterface();

	public static LocalInterface localInterface = null;

	static String jobHadoopJarURL, jobInputURL;
	public static String instanceType = null; 
	public static int instanceCount; 
	public static String bucketName = null;
	public static String station = null;
	public static WeatherPipeFileWriter fileWriter = new  WeatherPipeFileWriter();
	

	public static void main(String[] args) throws IOException {
		
		

		MapReduceBuilder builder = new MapReduceBuilder(null);
		
		MapReduceInterface mrInterface = addFlags(args);		

		String mapReduceJarLocation = builder.buildMapReduceJar();
		

		System.out.println("Searching NEXRAD Files");

		radarFileNames = RadarFilePicker.getRadarFilesFromTimeRange(startTime, endTime, station, awsAnonInterface, dataBucket);

		RadarFilePicker.executor.shutdown();
		while (!RadarFilePicker.executor.isTerminated()) {}
		//System.out.println("Finished all threads");
		
		
		System.out.println("Found " + radarFileNames.size() + " NEXRAD Radar files between " + startTime.toString()
				+ " and " + endTime.toString());
		
		//exit if the station's name cannot find
		if (radarFileNames.size() == 0) {
			System.out.println("There's no NEXRAD File found, please check the name of the station");
			System.exit(1);
		}
		
		System.out.println();
		System.out.println("Search for/Create WeatherPipe S3 bucket");
		bucketName = mrInterface.FindOrCreateWeatherPipeJobDirectory();
		if (bucketName == null) {
			System.out.println("Bucket was not created correctly");
			System.exit(1);
		}
		System.out.println("Using bucket " + bucketName);

		System.out.print("Uploading Input file... ");
		jobInputURL = mrInterface.UploadInputFileList(radarFileNames, dataBucket);

		System.out.print("Uploading Jar file... ");
		jobHadoopJarURL = mrInterface.UploadMPJarFile(mapReduceJarLocation);

		mrInterface.CreateMRJob(jobInputURL, jobHadoopJarURL, instanceCount, instanceType);

		try {
			fileWriter.writeOutput(mrInterface.jobOutput, mrInterface.jobDirName, mapReduceJarLocation);
		} catch (MalformedURLException | ClassNotFoundException | NoSuchMethodException | SecurityException
				| InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		mrInterface.close();

	}

	public static MapReduceInterface addFlags(String[] args) throws IOException {
		// create Options object
		
		INIEditor i = new INIEditor();
		i.read("WeatherPipe.ini");
		
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
		options.addOption("hathi", "LocalInterface", false, "add this if you want to use a local Hadoop cluster");

		try {
			// parse the command line arguments
			CommandLine line = parser.parse(options, args);

			if (line.hasOption("start_time") && (line.getOptionValue("start_time") != null)) {
				startTime = DateTime.parse(line.getOptionValue("start_time"), dateFormat);
				i.setValue("startTime", line.getOptionValue("start_time"));
			} else if (i.hasKey("startTime") && !i.getValue("startTime").equals("")) {
				startTime = DateTime.parse(i.getValue("startTime"), dateFormat);
			} else {
				System.out.println("Flag start_time is required");
				System.exit(1);
			}

			if (line.hasOption("end_time")) {
				endTime = DateTime.parse(line.getOptionValue("end_time"), dateFormat);
				i.setValue("endTime", line.getOptionValue("end_time"));
			} else if (i.hasKey("endTime") && !i.getValue("endTime").equals("")) {
				endTime = DateTime.parse(i.getValue("endTime"), dateFormat);
			} else {
				System.out.println("Flag end_time is required");
				System.exit(1);
			}

			if (line.hasOption("station") && RadarFilePicker.checkStationType(line.getOptionValue("station"))) {
				station = line.getOptionValue("station");
				i.setValue("station", station);
			} else if (i.hasKey("station") && RadarFilePicker.checkStationType(i.getValue("station"))) {
				station = i.getValue("station");
			} else {
				System.out.println("Flag station is required");
				System.exit(1);
			}

			if (line.hasOption("jobID")) {
				jobID = line.getOptionValue("jobID");
				i.setValue("jobID", jobID);
			} else if (i.hasKey("jobID") && !i.getValue("jobID").equals("")){
				jobID = i.getValue("jobID");
			}
			
			if (line.hasOption("instanceType")) {
				instanceType = line.getOptionValue("instanceType");
				i.setValue("instanceType", instanceType);
			} else if (i.hasKey("instanceType") && !i.getValue("instanceType").equals("")){
				instanceType = i.getValue("instanceType");
			}

			if (line.hasOption("instanceCount")) {
				instanceCount = Integer.parseInt(line.getOptionValue("instanceCount"));
				i.setValue("instanceCount", Integer.toString(instanceCount));
			} else if (i.hasKey("instanceCount") && !i.getValue("instanceCount").equals("")){
				instanceCount = Integer.parseInt(i.getValue("instanceCount"));
			} else {
				instanceCount = 1;
			}
			
			if(line.hasOption("hathi")) {
				System.out.println("Using Hathi");
				localInterface = new LocalInterface();
				return localInterface;
			}
			else if (line.hasOption("bucket_name")) {
				bucketName = line.getOptionValue("bucket_name");
				i.setValue("bucketName", bucketName);
				awsInterface = new AWSInterface(jobID, bucketName);
				return awsInterface;

			} 
			else {
				awsInterface = new AWSInterface(jobID);
				return  awsInterface;
			}

		} catch (ParseException exp) {
			System.out.println("Unexpected exception:" + exp.getMessage());
			System.exit(1);
		} catch (IllegalArgumentException e){
			System.out.println("Illegal Arguments with invalid format: " + e.getMessage());
			System.exit(1);
		}
		return null;

	}
}
