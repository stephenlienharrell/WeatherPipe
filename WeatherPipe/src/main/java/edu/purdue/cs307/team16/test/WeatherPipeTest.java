package edu.purdue.cs307.team16.test;


import edu.purdue.cs307.team16.*;
import junit.framework.TestCase;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

public class WeatherPipeTest extends TestCase{
	
	@Test
	public void testAddFlags(){
		final String dateFormatString = "dd/MM/yyyy HH:mm:ss";
		final DateTimeFormatter dateFormat = DateTimeFormat.forPattern(dateFormatString);
		String[] args = {"-s",  "01/01/2010 07:30:00", "-e",  "01/01/2010 23:00:00",  "-st", "KAKQ",
				"-id", "whateverid", "-b", "whateverbucket", "-i_T", "whatevertype", "-i_C", "42"};
		WeatherPipe.addFlags(args);
		
		assertEquals("KAKQ", WeatherPipe.station);
		assertEquals(DateTime.parse("01/01/2010 07:30:00", dateFormat), WeatherPipe.startTime);
		assertEquals(DateTime.parse("01/01/2010 23:00:00", dateFormat), WeatherPipe.endTime);
		assertEquals("whateverid", WeatherPipe.jobID);
		assertEquals("whateverbucket", WeatherPipe.bucketName);
		assertEquals("whatevertype", WeatherPipe.instanceType);
		assertEquals(42, WeatherPipe.instanceCount);
		System.out.println("addFlags is ok");
	}
}
