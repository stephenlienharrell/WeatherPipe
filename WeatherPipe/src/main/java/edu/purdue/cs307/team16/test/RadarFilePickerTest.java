package edu.purdue.cs307.team16.test;

import static org.junit.Assert.assertArrayEquals;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import org.joda.time.DateTime;
import org.junit.Test;

import edu.purdue.cs307.team16.AWSInterface;
import edu.purdue.cs307.team16.RadarFilePicker;
import junit.framework.TestCase;

public class RadarFilePickerTest extends TestCase {

	@Test
	public void testAddFile() {
		// setup function
		// run function
		// RadarFilePicker.addFile(ret, lowBound, uppBound, station, key,
		// awsInterface, dataBucket);
		// test output
		ArrayList<String> ret = new ArrayList<String>();
		String[] lowBound = {"20100101_000000", "20100714_1023027"};
		String[] uppBound = {"20100101_235959", "20100714_172619"};
		String[] key = {"2010/01/01", "2010/07/14"};
		String station = "KBBX";
		final String dataBucket = "noaa-nexrad-level2";
		final String jobID = null;
		AWSInterface awsInterface = new AWSInterface(jobID);
		int[] size = new int[2];
		for(int i = 0; i < 2; i++) {
			RadarFilePicker.addFile(ret, lowBound[i], uppBound[i], station, key[i], awsInterface, dataBucket);
			size[i] = ret.size();
			ret.clear();
		}
		int[] answer = {193,47};
		assertArrayEquals(answer, size);
		System.out.println("AddFile() is ok");
	}

	@Test
	public void testGetRadarFilesFromTimeRange() {
		// setup function
		// run function
		// RadarFilePicker.addFile(ret, lowBound, uppBound, station, key,
		// awsInterface, dataBucket);
		// test output
		int[] output = new int[3];
		final String dataBucket = "noaa-nexrad-level2";
		DateTime[] startTimes = { new DateTime(2010, 01, 01, 00, 00, 00), new DateTime(2010, 03, 01, 02, 30, 27),
				new DateTime(2010, 12, 31, 19, 18, 39) };
		DateTime[] endTimes = { new DateTime(2010, 01, 02, 00, 00, 00), new DateTime(2010, 03, 01, 17, 26, 19),
				new DateTime(2011, 01, 01, 15, 27, 48) };
		final String jobID = null;
		AWSInterface awsInterface = new AWSInterface(jobID);
		String station = "KBBX";
		ArrayList<String> radarFileNames;
		// int size;
		for (int i = 0; i < 3; i++) {
			radarFileNames = RadarFilePicker.getRadarFilesFromTimeRange(startTimes[i], endTimes[i], station,
					awsInterface, dataBucket);
			// size = radarFileNames.size();
			output[i] = radarFileNames.size();
		}
		int[] answer = {193, 74, 164};
		assertArrayEquals(answer, output);
		System.out.println("GetRadarFilesFromTimeRange() is ok");
	}

	
}