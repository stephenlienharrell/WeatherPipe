package edu.purdue.eaps.weatherpipe;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import java.lang.System;
import java.lang.Runtime;


import org.apache.commons.io.FileUtils;
import org.apache.log4j.PropertyConfigurator;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;


public class AWSInterface extends MapReduceInterface {

  
	private String jobBucketNamePrefix = "weatherpipe";
	private AmazonElasticMapReduce emrClient;
	private AmazonS3 s3client;
	private TransferManager transMan;
	private Region region;
	private String jobSetupDirName;
	private String jobLogDirName;
	//private String defaultInstance = "c3.xlarge";

	private String jobBucketName;
	private String jobID;
	
	private int bytesTransfered = 0;
	
	

	
	public AWSInterface(String job, String bucket){
		String weatherPipeBinaryPath = WeatherPipe.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		String log4jConfPath = weatherPipeBinaryPath.substring(0, weatherPipeBinaryPath.lastIndexOf("/")) + "/log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);
		jobBucketName = bucket;
		AwsBootstrap(job);
	}

	
	private void AwsBootstrap(String job) {
		AWSCredentials credentials;
		ClientConfiguration conf;
		String userID;
		MessageDigest md = null;
		byte[] shaHash;
		StringBuffer hexSha;
		DateFormat df;
		TimeZone tz;
		String isoDate;	
		File jobDir;
		File jobSetupDir;
		File jobLogDir;
		int i;
		
    	conf = new ClientConfiguration();
    	// 2 minute timeout
    	conf.setConnectionTimeout(120000);
    	
		credentials = new ProfileCredentialsProvider("default").getCredentials();
		// TODO: add better credential searching later
			
		region = Region.getRegion(Regions.US_EAST_1);
		s3client = new AmazonS3Client(credentials, conf);
		s3client.setRegion(region);
		
		transMan = new TransferManager(s3client);
		
		emrClient = new AmazonElasticMapReduceClient(credentials, conf);
		emrClient.setRegion(region);
		
		if(jobBucketName == null) {
			userID = new AmazonIdentityManagementClient(credentials).getUser().getUser().getUserId();
			try {
				md = MessageDigest.getInstance("SHA-256");
				md.update(userID.getBytes("UTF-8"));
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			shaHash = md.digest();
			hexSha = new StringBuffer();
			for(byte b : shaHash) {
				hexSha.append(String.format("%02X", b));		
			}
		
			jobBucketName = jobBucketNamePrefix + "." + hexSha;
			if(jobBucketName.length() > 63) {
				jobBucketName = jobBucketName.substring(0,62);
			}
		
		}
		
		jobBucketName = jobBucketName.toLowerCase();
		
		if(job == null) {
		    tz = TimeZone.getTimeZone("UTC");
		    df = new SimpleDateFormat("yyyy-MM-dd'T'HH.mm");
		    df.setTimeZone(tz);
		    isoDate = df.format(new Date());
			jobID = isoDate + "." + Calendar.getInstance().get(Calendar.MILLISECOND);
			
		//  UUID Code if date isn't good	
		//	jobID = UUID.randomUUID().toString();
		} else {
			jobID = job;
		}
		
		jobDirName = "WeatherPipeJob" + jobID;
		jobDir = new File(jobDirName);
        i = 0;
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
	}
	
	private void UploadFileToS3(String jobBucketName, String key, File file) {
		Upload upload;
		PutObjectRequest request;
		
		request = new PutObjectRequest(
				jobBucketName, key, file);
		
		

		bytesTransfered = 0;
		// Subscribe to the event and provide event handler.        
		request.setGeneralProgressListener(new ProgressListener() {

			@Override
			public void progressChanged(ProgressEvent progressEvent) {
				bytesTransfered += progressEvent.getBytesTransferred();
				
			}

		});
		
		System.out.println();
		upload = transMan.upload(request);
		
		while(!upload.isDone()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				continue;
			}
			
			System.out.print("\rTransfered: " + bytesTransfered/1024 + "K / " + file.length()/1024 + "K");
		}
		// If we got an error the count could be off
		System.out.print("\rTransfered: " + bytesTransfered/1024 + "K / " + bytesTransfered/1024 + "K");
		System.out.println();
		System.out.println("Transfer Complete");

	}
	
	public String FindOrCreateWeatherPipeJobDirectory() {
		String bucketLocation = null;

		try {
            if(!(s3client.doesBucketExist(jobBucketName))) {
            	// Note that CreateBucketRequest does not specify region. So bucket is 
            	// created in the region specified in the client.
            	s3client.createBucket(new CreateBucketRequest(
						jobBucketName));
            
            } else {
            	s3client.headBucket(new HeadBucketRequest(jobBucketName));
            }

            bucketLocation = "s3n://" + jobBucketName + "/";
            
         } catch (AmazonServiceException ase) {
        	 if(ase.getStatusCode() == 403) {
        		 System.out.println("You do not have propper permissions to access " + jobBucketName + 
        				 	". S3 uses a global name space, please make sure you are using a unique bucket name.");
        		 System.exit(1);
        	 } else {
        	 
        		 System.out.println("Caught an AmazonServiceException, which " +
        				 "means your request made it " +
        				 "to Amazon S3, but was rejected with an error response" +
        				 " for some reason.");
        		 System.out.println("Error Message:    " + ase.getMessage());
        		 System.out.println("HTTP Status Code: " + ase.getStatusCode());
        		 System.out.println("AWS Error Code:   " + ase.getErrorCode());
        		 System.out.println("Error Type:       " + ase.getErrorType());
        		 System.out.println("Request ID:       " + ase.getRequestId());
        	 }
        	 System.exit(1);
        		 
         } catch (AmazonClientException ace) {
             System.out.println("Caught an AmazonClientException, which " +
             		"means the client encountered " +
                     "an internal error while trying to " +
                     "communicate with S3, " +
                     "such as not being able to access the network.");
             System.out.println("Error Message: " + ace.getMessage());
             System.exit(1);
         }
		return bucketLocation;	
	}
	
	public String UploadInputFileList(ArrayList<String> fileList, String dataDirName) {
		
		String key = jobID + "_input";
		String uploadFileString = "";
		PrintWriter inputFile = null;
		File file = new File(jobSetupDirName + "/" + key);
		
		
		for (String s : fileList) uploadFileString += dataDirName + " " + s + "\n";
	
		
		try {
			inputFile = new PrintWriter(file);
			inputFile.print(uploadFileString);
			inputFile.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}	
		
		UploadFileToS3(jobBucketName, key, file);
		
		return "s3n://" + jobBucketName + "/" + key;
	}
	
	
	public String UploadMPJarFile(String fileLocation) {
		String key = jobID + "WeatherPipeMapreduce.jar";
		File jarFile = new File(fileLocation);
		
		UploadFileToS3(jobBucketName, key, jarFile);
		
		try {
			FileUtils.copyFile(new File(fileLocation), new File(jobSetupDirName + "/" + key));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		
		return "s3n://" + jobBucketName + "/" + key;
	}

	public void CreateMRJob(String jobInputLocation, String jobJarLocation, int numInstances, String instanceType) {
		
		// Modified from https://mpouttuclarke.wordpress.com/2011/06/24/how-to-run-an-elastic-mapreduce-job-using-the-java-sdk/
		
		// first run aws emr create-default-roles

		String hadoopVersion = "2.4.0";
		String flowName = "WeatherPipe_" + jobID;
		String logS3Location = "s3n://" + jobBucketName + "/" + jobID + ".log";
		String outS3Location = "s3n://" + jobBucketName + "/" + jobID + "_output";
		String[] arguments = new String[] {jobInputLocation, outS3Location};
		List<String> jobArguments = Arrays.asList(arguments);
		DescribeClusterRequest describeClusterRequest = new DescribeClusterRequest();
		DescribeClusterResult describeClusterResult;
		File rawOutputFile = new File(jobDirName + "/" + jobID + "_raw_map_reduce_output");
		File localLogDir = new File(jobLogDirName);
		int normalized_hours;
		double cost;
		long startTimeOfProgram, endTimeOfProgram, elapsedTime;
		final String resultId;

		String line, lastStateMsg;
		StringBuilder jobOutputBuild;
		int i;
		Download download;
		int fileLength;
		

		BufferedReader lineRead;

		MultipleFileDownload logDirDownload;

		startTimeOfProgram = System.currentTimeMillis();
		
		if(instanceType == null) {
			instanceType = "c3.xlarge";
			System.out.println("Instance type is set to default: " + instanceType);
			System.out.println();
			
		}
		
        try {
            // Configure instances to use
            JobFlowInstancesConfig instances = new JobFlowInstancesConfig();
            System.out.println("Using EMR Hadoop v" + hadoopVersion);
            instances.setHadoopVersion(hadoopVersion);
            System.out.println("Using instance count: " + numInstances);
            instances.setInstanceCount(numInstances);
            System.out.println("Using master instance type: " + instanceType);
            instances.setMasterInstanceType("c3.xlarge");
            
            // do these need to be different??
            System.out.println("Using slave instance type: " + instanceType);
            instances.setSlaveInstanceType(instanceType);
            

            // Configure the job flow
            System.out.println("Configuring flow: " + flowName);
            RunJobFlowRequest request = new RunJobFlowRequest(flowName, instances);
            System.out.println("\tusing log URI: " + logS3Location);
            request.setLogUri(logS3Location);
            request.setServiceRole("EMR_DefaultRole");
            request.setAmiVersion("3.1.0");
            // this may change for some people
            
      
            request.setJobFlowRole("EMR_EC2_DefaultRole");

            System.out.println("\tusing jar URI: " + jobJarLocation);
            HadoopJarStepConfig jarConfig = new HadoopJarStepConfig(jobJarLocation);
            System.out.println("\tusing args: " + jobArguments);
            jarConfig.setArgs(jobArguments);
            StepConfig stepConfig =
                new StepConfig(jobJarLocation.substring(jobJarLocation.indexOf('/') + 1),
                               jarConfig);
            request.setSteps(Arrays.asList(new StepConfig[] { stepConfig }));
            System.out.println("Configured hadoop jar succesfully!\n");

            //Run the job flow
            RunJobFlowResult result = emrClient.runJobFlow(request);
            System.out.println("Trying to run job flow!\n");
     
            describeClusterRequest.setClusterId(result.getJobFlowId());
            
            resultId = result.getJobFlowId();
           
            //Check the status of the running job
            String lastState = "";
           Runtime.getRuntime().addShutdownHook(new Thread() {public void run()
            	{	List<String> jobIds = new ArrayList<String>();
            		jobIds.add(resultId);
        	   		TerminateJobFlowsRequest tjfr = new TerminateJobFlowsRequest(jobIds);
        	   		emrClient.terminateJobFlows(tjfr);
        	   		System.out.println();
        	   		System.out.println("Amazon EMR job shutdown");
            	}});
           
           
            while (true)
            {
            	describeClusterResult = emrClient.describeCluster(describeClusterRequest);
            	Cluster cluster = describeClusterResult.getCluster();
            	lastState = cluster.getStatus().getState();

            	lastStateMsg = "\rCurrent State of Cluster: " + lastState;
            	System.out.print(lastStateMsg + "                                    ");

            	if(!lastState.startsWith("TERMINATED")) {
            		lastStateMsg = lastStateMsg + " ";
            		for(i = 0; i < 10; i++) {
            			lastStateMsg = lastStateMsg + ".";
            			System.out.print(lastStateMsg);
            			Thread.sleep(1000);
            		}
            		continue;
            	} else {	
            		lastStateMsg = lastStateMsg + " ";
            		System.out.print(lastStateMsg);
            	}
            	
            	// it reaches here when the emr has "terminated"
            	
            	normalized_hours = cluster.getNormalizedInstanceHours();
        		cost = normalized_hours * 0.011;
       	  	    endTimeOfProgram = System.currentTimeMillis(); // returns milliseconds
    		    elapsedTime = (endTimeOfProgram - startTimeOfProgram)/(1000);
    		 
        		
            	logDirDownload = transMan.downloadDirectory(jobBucketName, jobID + ".log", localLogDir);
            	
        		
        		while(!logDirDownload.isDone()) {
        			Thread.sleep(1000);
        		}
        		System.out.println();
        		
        		
        		
            	if(!lastState.endsWith("ERRORS")) {	   
            		bytesTransfered = 0;
            		fileLength = (int)s3client.getObjectMetadata(jobBucketName, jobID + "_output" + "/part-r-00000").getContentLength();
            		GetObjectRequest fileRequest = new GetObjectRequest(jobBucketName, jobID + "_output" + "/part-r-00000");
            		fileRequest.setGeneralProgressListener(new ProgressListener() {

            			@Override
            			public void progressChanged(ProgressEvent progressEvent) {
            				bytesTransfered += progressEvent.getBytesTransferred();
            				
            			}

            		});
            		
            		download = transMan.download(new GetObjectRequest(jobBucketName, jobID + "_output" + "/part-r-00000"), rawOutputFile);
            		
            		
            		System.out.println("Downloading Output"); 		
            		while(!download.isDone()) {
            			try {
            				Thread.sleep(1000);
            			} catch (InterruptedException e) {
            				continue;
            			}
            			
           // 			System.out.print("\rTransfered: " + bytesTransfered/1024 + "K / " + fileLength/1024 + "K          ");
            		}
            	
           /*  Printing this stuff isn't working	
            		// If we got an error the count could be off
            		System.out.print("\rTransfered: " + bytesTransfered/1024 + "K / " + bytesTransfered/1024 + "K           ");
            		System.out.println();
           */
            		System.out.println("Transfer Complete");
		
            		
            		System.out.println("The job has ended and output has been downloaded to " + jobDirName);
            		System.out.printf("Normalized instance hours: %d\n", normalized_hours);
            		System.out.printf("Approximate cost of this run: $%2.02f\n", cost);
            		System.out.println("The job took " + elapsedTime + " seconds to finish" );
            		
            		lineRead = new BufferedReader(new FileReader(rawOutputFile));
            		

            		jobOutputBuild = new StringBuilder("");
            		while((line = lineRead.readLine()) != null) {
            			if(line.startsWith("Run#")) {
            				jobOutputBuild = new StringBuilder("");
            				jobOutputBuild.append(line.split("\t")[1]);
            			} else {
            			    jobOutputBuild.append("\n");
            			    jobOutputBuild.append(line);
            			}
            			
            		}
            		
            		jobOutput = jobOutputBuild.toString();
            		break;
            	}

        		jobOutput = "FAILED";
            	System.out.println("The job has ended with errors, please check the log in " + localLogDir);
            	System.out.printf("Normalized instance hours: %d\n", normalized_hours);
            	System.out.printf("Approximate cost of this run = $%2.02f\n", cost);
            	System.out.println("The job took " + elapsedTime + " seconds to finish" );
            	
            	break;
                
            }
        } catch (AmazonServiceException ase) {
                System.out.println("Caught Exception: " + ase.getMessage());
                System.out.println("Reponse Status Code: " + ase.getStatusCode());
                System.out.println("Error Code: " + ase.getErrorCode());
                System.out.println("Request ID: " + ase.getRequestId());
        } catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    
        
    }
	
	public void addJobBucketName (String jobBucketName){
		this.jobBucketName = jobBucketName;
	}	

	protected void close() {
		transMan.shutdownNow();
	}
}
