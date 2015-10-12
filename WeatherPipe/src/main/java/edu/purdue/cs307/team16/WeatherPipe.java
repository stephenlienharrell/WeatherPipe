package edu.purdue.cs307.team16;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

//import edu.purdue.cs307.team16.S3FileReadPrototype;
		 
public class WeatherPipe {

	public static void main(String[] args) {

		 final String dateFormatString = "dd/MM/yyyy HH:mm:ss";
		 final String dateDesc = "Date Format is " + dateFormatString;
		 final DateTimeFormatter dateFormat = DateTimeFormat.forPattern(
			dateFormatString);
		 DateTime startTime;
		 DateTime endTime;
	
		 // create Options object
		 Options options = new Options();
		 CommandLineParser parser = new DefaultParser();
		

		 
		 // add t option
		 options.addOption("start_time", true, "Start time of analysis. " + dateDesc);
		 options.addOption("end_time", true, "End time of analysis. " + dateDesc);
		 
		 try {
			 // parse the command line arguments
			 CommandLine line = parser.parse( options, args );
		 
			 if( line.hasOption( "start_time" ) ) {
				startTime = DateTime.parse(
					line.getOptionValue("start_time"), 
					dateFormat);
			 }
			 if( line.hasOption( "end_time" ) ) {
				endTime = DateTime.parse(
					line.getOptionValue("end_time"), 
					dateFormat);
			 }
		 } catch( ParseException exp ) {
			 System.out.println( "Unexpected exception:" + exp.getMessage() );
		 }

		 // send DateTime objects to s3 file lister
		 // send list of files to emr starter
	
	
	}
}
