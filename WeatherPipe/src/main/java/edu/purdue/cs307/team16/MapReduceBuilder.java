package edu.purdue.cs307.team16;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;


public class MapReduceBuilder {
	
	String gradleBinary = null;
	String weatherPipeMapReduceDir = null;

	public MapReduceBuilder(String gradleBin) {
		if(gradleBin != null) {
			gradleBinary = gradleBin;
		}
	}
	
	String buildMapReduceJar() {
		
		
		findGradlePath();
		findWeatherPipeMapReduceBuildDir();
		System.out.println("Attempting to build Map Reduce with");
		System.out.println("gradle: " + gradleBinary);
		System.out.println("build directory: " + weatherPipeMapReduceDir);
		System.out.println();
		String weatherPipeJarLocation = weatherPipeMapReduceDir + "/build/libs/WeatherPipeMapReduce.jar";
		Process command = null;
		final String[] args = {gradleBinary, "build"};
		final String[] env = { "JAVA_HOME=" + System.getProperty("java.home") };
		BufferedReader buildOut;
		String buildLine;
		
		try {
			command = Runtime.getRuntime().exec(args, env, new File(weatherPipeMapReduceDir));	
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.exit(1);
		}
		try {
			
			buildOut = new BufferedReader(new InputStreamReader(command.getInputStream()));
			
			while((buildLine = buildOut.readLine()) != null) {
				System.out.println(buildLine);				
			}
		} catch (IOException e1) {
			
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.exit(1);
		}
		try {
			command.waitFor();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			sys.exit(1);
		}
		if(command.exitValue() == 1) {
			System.out.println("Map Reduce Jar Build Failed");
			System.exit(1);
		}
		System.out.println("Build completed");
		System.out.println("MapReduce jar location: " + weatherPipeJarLocation);
		System.out.println();
		
		return weatherPipeJarLocation;
	}
	
	// should take a flag to override this
	void findGradlePath() {
		Process command = null;
		
		if(gradleBinary == null ){ 
			try {
				command = Runtime.getRuntime().exec("which gradle");	
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				System.exit(1);
			}
			try {
				gradleBinary = new BufferedReader(new InputStreamReader(command.getInputStream())).readLine();
			} catch (IOException e1) {
			
				// TODO Auto-generated catch block
				e1.printStackTrace();
				System.exit(1);
			}
		}
			
	}

	// probably should take a flag to override this 
	void findWeatherPipeMapReduceBuildDir() {
		String path = WeatherPipe.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		String decodedPath = "";
			 
		 
	 	try {
			decodedPath = URLDecoder.decode(path, "UTF-8");
	 	} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
 			e.printStackTrace();
	 		System.exit(1);
		}
	 	// Only works on Linux and Mac
	 	// expecting a long directory like /home/user/thing/WeatherPipe/lib/Weatherpipe.jar
	 	// just need /home/user/thing/WeatherPipe/
	 	
	 	// probably need some better checking here
	 	weatherPipeMapReduceDir = decodedPath.substring(0, 
	 			decodedPath.substring(0, 
	 					decodedPath.lastIndexOf("/")).lastIndexOf("/")) + "/WeatherPipeMapReduce";	
	}
	
}
