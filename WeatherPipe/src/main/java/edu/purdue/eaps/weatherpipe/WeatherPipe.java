package edu.purdue.eaps.weatherpipe;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalINIConfiguration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


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
		while (!RadarFilePicker.executor.isTerminated())
			try {
				Thread.sleep(500);
			} catch (InterruptedException e1) {

			}
		//System.out.println("Finished all threads");
		
		
		System.out.println("Found " + radarFileNames.size() + " NEXRAD Radar files between " + startTime.toString()
				+ " and " + endTime.toString());
		
		//exit if the station's name cannot find
		if (radarFileNames.size() == 0) {
			System.out.println("No Data found for NEXRAD station " + station + " with the given start and end times");
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

	public static MapReduceInterface addFlags(String[] args) {
		// create Options object
		

		Configuration config;
		Iterator<String> keys;
		String key;
		Options options = new Options();
		CommandLineParser parser = new DefaultParser();
		HelpFormatter helpFormatter = new HelpFormatter();
		String helpHeader = "";
		String helpFooter = "\nPlease report issues at https://github.com/stephenlienharrell/WeatherPipe/issues";
		
		
		// add options for jar file and radar station if time is available

		options.addOption("b", "bucket_name", true,
				"Bucket name in S3 to place input and output data. Will be auto-generated if not given");
		options.addOption("s", "start_time", true, "Start search boundary for NEXRAD data search. " + dateDesc);
		options.addOption("e", "end_time", true, "End search boundary for NEXRAD data search. " + dateDesc);
		options.addOption("st", "station", true,
				"Radar station abbreviation ex. \"KIND\"");
		options.addOption("id", "job_id", true, "Name of this particular job, a random one will be generated if not given. This must be unique in reference to other jobs.");
		options.addOption("t", "instance_type", true,
				"Instance type for EMR job. Default is c3.xlarge. See options here: https://aws.amazon.com/elasticmapreduce/pricing/ ");
		options.addOption("i", "instance_count", true, "The amount of instances to run the analysis on. Default is 1.");
		options.addOption("c", "config_file", true, "Location of config file");
		options.addOption("h", "help", false, "Print this help message");
		
		
		try {

			// parse the command line arguments
			CommandLine line = parser.parse(options, args);
			
			if(line.hasOption("help")) { 
				helpFormatter.printHelp("WeatherPipe", helpHeader, options, helpFooter, true);
				System.exit(0);
			}
		
			for(Option option: line.getOptions()) {
				if((option.hasArg()) && (option.getValue() == null)) {
					throw new MissingArgumentException("Option " + option.getArgName() + " requires an argument.");
				}
			}
			
			try {			
				if(!line.hasOption("config_file") && !new File("WeatherPipe.ini").exists()) {
					config = new HierarchicalINIConfiguration();	
				} else if(line.hasOption("config_file")) {
					config = new HierarchicalINIConfiguration(line.getOptionValue("config_file"));
				} else {
					config = new HierarchicalINIConfiguration("WeatherPipe.ini");
				}
			} catch (ConfigurationException e) {
				config = new HierarchicalINIConfiguration();
			}
			
			keys = config.getKeys();
			while(keys.hasNext()) {
				key = keys.next();
				System.out.println("key: " + key + ", value:" + config.getString(key));
				if(config.getString(key).equals("")) {
					config.clearProperty(key);
				}
			}

			if (line.hasOption("start_time")) {
				config.clearProperty("analysis.start_time");
				config.addProperty("analysis.start_time", line.getOptionValue("start_time"));
			} else if(config.getString("analysis.start_time", null) == null) {
				throw new MissingOptionException("start_time is a required flag or setting in the config file");
			}
			
			if (line.hasOption("end_time")) {
				config.clearProperty("analysis.end_time");
				config.addProperty("analysis.end_time", line.getOptionValue("end_time"));
			} else if(config.getString("analysis.end_time", null) == null) {
				throw new MissingOptionException("end_time is a required flag or setting in the config file");
			}
			
			try {
				startTime = DateTime.parse(config.getString("analysis.start_time"), dateFormat);
				endTime = DateTime.parse(config.getString("analysis.end_time"), dateFormat);
			} catch (IllegalArgumentException e) {
				System.out.println("Invalid Date Format: The proper format is " + dateDesc);
				helpFormatter.printHelp("WeatherPipe", helpHeader, options, helpFooter, true);
				System.exit(1);
			}
			
			
			if (line.hasOption("station")) {
				config.clearProperty("analysis.station");
				config.addProperty("analysis.station", line.getOptionValue("station"));
			} else if(config.getString("analysis.station", null) == null) {
				throw new MissingOptionException("station is a required flag or setting in the config file");
			} 
			if(!RadarFilePicker.checkStationType(config.getString("analysis.station"))) {
				throw new ParseException("Radar station must be 4 captial letters. ex. KIND");
			}
			station = config.getString("analysis.station");

			
			if (line.hasOption("instance_type")) {
				config.clearProperty("emr.instance_type");
				config.addProperty("emr.instance_type", line.getOptionValue("instance_type"));
			}
			instanceType = config.getString("emr.instance_type", null);
			
			if (line.hasOption("instance_count")) {
				config.clearProperty("emr.instance_count");
				config.addProperty("emr.instance_count", Integer.parseInt(line.getOptionValue("instance_count")));
			}
			instanceCount = config.getInt("emr.instance_count", 1);
			
			if (line.hasOption("bucket_name")) {
				config.clearProperty("emr.bucket_name");
				config.addProperty("emr.bucket_name", line.getOptionValue("bucket_name"));
			}
			bucketName = config.getString("emr.bucket_name", null);
				
			// jobID shouldn't be a config file property
			
			if (line.hasOption("job_id")) jobID = line.getOptionValue("job_id");


			awsInterface = new AWSInterface(jobID, bucketName);
			
			/*
			if(line.hasOption("hathi")) {
				System.out.println("Using Hathi");
				localInterface = new LocalInterface();
				return localInterface;
			}
			*/
			
			
		} catch(MissingOptionException e) {
			System.out.println("Missing Option Error: " + e.toString());
			helpFormatter.printHelp("WeatherPipe", helpHeader, options, helpFooter, true);
			System.exit(1);
		} catch(MissingArgumentException e) {
			System.out.println("Missing Argument Error: " + e.toString());
			helpFormatter.printHelp("WeatherPipe", helpHeader, options, helpFooter, true);
			System.exit(1);
		} catch (ParseException e) {
			System.out.println("Command Line Parsing Error: " + e.toString());
			helpFormatter.printHelp("WeatherPipe", helpHeader, options, helpFooter, true);
			System.exit(1);
		} 

		return awsInterface;
	}

}
