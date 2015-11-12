package edu.purdue.cs307.team16.test;

import junit.framework.TestSuite; 
import junit.framework.Test; 
import junit.textui.TestRunner; 

public class TestAll extends TestSuite {
	public static Test suite() { 
        TestSuite suite = new TestSuite("TestSuite Test"); 
        suite.addTestSuite(AWSInterfaceTest.class); 
        suite.addTestSuite(RadarFilePickerTest.class);
        suite.addTestSuite(WeatherPipeTest.class); 
        return suite; 
    } 
    public static void main(String args[]){ 
        TestRunner.run(suite()); 
    } 
}
