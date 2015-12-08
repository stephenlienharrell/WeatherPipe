package edu.purdue.eaps.weatherpipe;

import java.io.FileReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.io.FileUtils;

public class LocalInterface extends MapReduceInterface {

	private String jobSetupDirName;
	private String jobLogDirName;
	public String jobOutputDirName;
	File jobDir;
	File jobSetupDir;
	File jobLogDir;
	File jobOutputDir;


	// name of folder 	
	private String hathiFolder = null;
	private String jobID;


	MessageDigest md = null;
	byte[] shaHash;
	StringBuffer hexSha;
	String user = null;
	Process p = null;
	String rcacScratch = null;

	LocalInterface() {

		// set jobbucketname
		try {
			String [] commands = {"bash", "-c", "echo $USER" };
			p = Runtime.getRuntime().exec(commands);
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			p.waitFor();
			while((user = in.readLine()) != null) {
				break;
			}
	
			in.close();

			String [] commands2 = {"bash", "-c", "echo $RCAC_SCRATCH" };
			p = Runtime.getRuntime().exec(commands2);
			BufferedReader in2 = new BufferedReader(new InputStreamReader(p.getInputStream()));
			p.waitFor();
			while((rcacScratch = in2.readLine()) != null) {
				break;
			}
			
			in2.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 


		hathiFolder = "WeatherPipeRuns";
		
		// generate jobID
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH.mm");
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		String isoDate = df.format(new Date());
		jobID =	isoDate + "." + Calendar.getInstance().get(Calendar.MILLISECOND);

		// make local file structure
		jobDirName = "WeatherPipeJob" + jobID;
		System.out.println("jobDirName = " + jobDirName);
		jobDir = new File(jobDirName);
		int i = 0;
		while(jobDir.exists()) {
			i++;
			jobDirName = jobDirName + "-" + i;
			jobDir = new File(jobDirName);
		}

		jobDir.mkdir();

		jobSetupDirName = jobDirName + "/" + "job_setup";
		jobSetupDir = new File(jobSetupDirName);
		jobSetupDir.mkdir();

		jobLogDirName = jobDirName + "/" + "logs";
		jobLogDir = new File(jobLogDirName);
		jobLogDir.mkdir();
		
		jobOutputDirName = jobDirName + "/output";
		jobOutputDir = new File(jobOutputDirName);
		jobOutputDir.mkdir();
		
	}



	// create directory on hdfs dfs 
	public String FindOrCreateWeatherPipeJobDirectory() {

		// check if directory exists or make new directory
		String [] commands = {"bash", "-c", "hdfs dfs -ls | grep " + hathiFolder };
		boolean folderExists = false;
		try {

			Process p = Runtime.getRuntime().exec(commands);
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			p.waitFor();
			if((in.readLine()) != null) {
				//System.out.println("out = " + out);
				folderExists = true;
			}
		} catch(IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}


		if(!folderExists) {
			// create hathiFolder

			try {
				//System.out.println("creating hathiFolder");
				p = Runtime.getRuntime().exec("hdfs dfs -mkdir " + hathiFolder);
				p.waitFor();
				folderExists = true;
			} catch(IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		return rcacScratch + "/"  + hathiFolder;
	}

	// upload input file list to hathi
	public String UploadInputFileList(ArrayList<String> fileList, String dataDirName) {

		// make string
		String inputFilename = jobID + "_input";
		String printString = "";

		boolean first = true;
		for(String s : fileList) {
			if(!first) {
				printString += "\n";
			}
			printString += dataDirName + " " + s;
			first = false;
		}

		// setup local
		PrintWriter writer;
		try {
			writer = new PrintWriter(jobSetupDirName + "/" + inputFilename, "UTF-8");
			writer.println(printString);
			writer.close();
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			e.printStackTrace();
		}


		// setup Hathi
		String [] commands = {"bash", "-c", "hdfs dfs -copyFromLocal " + 
				jobSetupDirName + "/" + inputFilename + " " + "$RCAC_SCRATCH/" + hathiFolder + "/"};

		Process p;
		try {
			p = Runtime.getRuntime().exec(commands);
			InputStream error = p.getErrorStream();
			p.waitFor();
			if(error.available() != 0) {
				System.err.print("Upload input failed");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return hathiFolder + "/" + inputFilename;
	}


	public String UploadMPJarFile(String fileLocation) {

		// no need to upload to Hathi, is used from local node

		String jarFilename = jobID + "WeatherPipeMapreduce.jar";
		new File(fileLocation);

		// setup local
		try {
			FileUtils.copyFile(new File(fileLocation), new File(jobSetupDirName + "/" + jarFilename));
		} catch (IOException e) {
			System.err.print("Fileutils copying jar to local dir failed");
			e.printStackTrace();
		}
		return jobSetupDirName + "/" + jarFilename;
	}

	public void CreateMRJob(String jobInputLocation, String jobJarLocation, int numInstances, String instanceType) {

		String outputFilename = jobID + "_output";

		jobOutput = hathiFolder + "/" + outputFilename;
		
		//System.out.println("running: " +  "hadoop jar " + jobJarLocation + " " + jobInputLocation + " " + jobOutput);
		String [] commands = {"bash", "-c", "hadoop jar " + jobJarLocation + " " + jobInputLocation + " " + jobOutput};
		
		
		ProcessBuilder pb = new ProcessBuilder(commands);
		pb.redirectErrorStream(true);

		Process proc = null;
		try {
			proc = pb.start();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		System.out.println("Map Reduce starting ... !");

		String line;             
		BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));             
		try {
			while ((line = in.readLine()) != null) {
			    System.out.println(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		
		proc.destroy();
		System.out.println("Map Reduce ending ... !");
		
		
		
		// copy files back into local directory
		String [] commands2 = {"bash", "-c", "hdfs dfs -copyToLocal " + 
				jobOutput + "/* " + jobOutputDirName + "/"};

		System.out.println("Downloading output files");
		
		ProcessBuilder pb2 = new ProcessBuilder(commands2);
		pb2.redirectErrorStream(true);
		Process proc2 = null;
		try {
			proc2 = pb2.start();
			proc2.waitFor();
			proc2.destroy();
			
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		} 

		String rawOutputFile = jobOutputDirName + "/part-r-00000";

		line = "";
		System.out.println("file download completed");
		BufferedReader lineRead = null;
		try {
			lineRead = new BufferedReader(new FileReader(rawOutputFile));
		}
		catch (FileNotFoundException f) {
			System.out.println("rawOutputFile not found");
			f.printStackTrace();
		}
		
		StringBuilder jobOutputBuild = new StringBuilder("");

		try {
			while((line = lineRead.readLine()) != null) {
				if(line.startsWith("Run#")) {
					jobOutputBuild = new StringBuilder("");
					jobOutputBuild.append(line.split("\t")[1]);
				} else {
				    jobOutputBuild.append("\n");
				    jobOutputBuild.append(line);
				}
				
			}
		}
		catch(IOException e) {
			System.out.println("Could not read line from file");
			e.printStackTrace();
		}

        jobOutput = jobOutputBuild.toString();


		System.out.println("job ended ...");
		
	}
	
	protected void close() {}

}
