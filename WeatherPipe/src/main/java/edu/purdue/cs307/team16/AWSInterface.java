package edu.purdue.cs307.team16;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.input.ReversedLinesFileReader;
import org.json.JSONObject;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
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
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;


public class AWSInterface {

  
	private String jobBucketNamePrefix = "weatherpipe";
	private String jobBucketName;
	private String jobID;
	private AmazonElasticMapReduce emrClient;
	private AmazonS3 s3client;
	Region region;
	
	public AWSInterface(String job){
		AwsBootstrap(job);
	}
	
	public AWSInterface(String job, String bucket){
		AwsBootstrap(job); 
		jobBucketName = bucket;
	}
	
	private void AwsBootstrap(String job) {
		AWSCredentials credentials;
		String userID;
	
		
		credentials = new ProfileCredentialsProvider("default").getCredentials();
		// add better credential searching later
		
		userID = new AmazonIdentityManagementClient(credentials).getUser().getUser().getUserId();
		
		region = Region.getRegion(Regions.US_EAST_1);
		s3client = new AmazonS3Client(credentials);
		s3client.setRegion(region);
		
		emrClient = new AmazonElasticMapReduceClient(credentials);
		emrClient.setRegion(region);
		
		jobBucketName = jobBucketNamePrefix + "." + userID;
		jobBucketName = jobBucketName.toLowerCase();
		
		jobID = job;
	}
	
	public List<S3ObjectSummary> ListBucket(String bucketName, String key) {
		
		ObjectListing listing = s3client.listObjects( bucketName, key );
		List<S3ObjectSummary> summaries = listing.getObjectSummaries();

		while (listing.isTruncated()) {
		   listing = s3client.listNextBatchOfObjects (listing);
		   summaries.addAll(listing.getObjectSummaries());
		}
		
		return summaries;
	}
	
	public String FindOrCreateWeatherPipeJobBucket() {
		String bucketLocation = null;

		try {
            if(!(s3client.doesBucketExist(jobBucketName))) {
            	// Note that CreateBucketRequest does not specify region. So bucket is 
            	// created in the region specified in the client.
            	s3client.createBucket(new CreateBucketRequest(
						jobBucketName));
            }

            bucketLocation = "s3n://" + jobBucketName + "/";
            
         } catch (AmazonServiceException ase) {
             System.out.println("Caught an AmazonServiceException, which " +
             		"means your request made it " +
                     "to Amazon S3, but was rejected with an error response" +
                     " for some reason.");
             System.out.println("Error Message:    " + ase.getMessage());
             System.out.println("HTTP Status Code: " + ase.getStatusCode());
             System.out.println("AWS Error Code:   " + ase.getErrorCode());
             System.out.println("Error Type:       " + ase.getErrorType());
             System.out.println("Request ID:       " + ase.getRequestId());
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
	
	public String UploadInputFileList(ArrayList<String> fileList, String dataBucketName) {
		
		String key = jobID + "_input";
		ObjectMetadata objMeta = new ObjectMetadata();
		String uploadFileString = "";
		InputStream uploadFileStream;
		
		for (String s : fileList)
		{
			uploadFileString += dataBucketName + " " + s + "\n";
		}
		
		uploadFileStream = new ByteArrayInputStream(uploadFileString.getBytes(Charset.forName("UTF-8")));
		
		// may need to set content size
		objMeta.setContentType("text/plain");
		try {
			s3client.putObject(jobBucketName, key, uploadFileStream, objMeta);
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
            		"means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
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
		return "s3n://" + jobBucketName + "/" + key;
	}
	
	public String UploadMPJarFile(String fileLocation) {
		String key = jobID + "WeatherPipeMapreduce.jar";
		File jarFile = new File(fileLocation);
		
		try {
			s3client.putObject(jobBucketName, key, jarFile);
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
            		"means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
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
		
		return "s3n://" + jobBucketName + "/" + key;
	}

	public void CreateEMRJob(String jobInputS3Location, String jobJarS3Location, int numInstances, String instanceType) {
		
		// Modified from https://mpouttuclarke.wordpress.com/2011/06/24/how-to-run-an-elastic-mapreduce-job-using-the-java-sdk/
		
		// first run aws emr create-default-roles
		
		String hadoopVersion = "2.4.0";
		String flowName = "WeatherPipe_" + jobID;
		String logS3Location = "s3n://" + jobBucketName + "/" + jobID + ".log";
		String outS3Location = "s3n://" + jobBucketName + "/" + jobID + "_output";
		String[] arguments = new String[] {jobInputS3Location, outS3Location};
		List<String> jobArguments = Arrays.asList(arguments);
		DescribeClusterRequest describeClusterRequest = new DescribeClusterRequest();
		DescribeClusterResult describeClusterResult;
		File errorLog = new File("WeatherPipeLog" + jobID + ".log");
		File outputFile = new File("WeatherPipeOut" + jobID + ".out");
		ReversedLinesFileReader revLineRead;
		String finalAverage;
		JSONObject jsonObj = new JSONObject();
		FileWriter fileWriter; 
		
        try {
            // Configure instances to use
            JobFlowInstancesConfig instances = new JobFlowInstancesConfig();
            System.out.println("Using EMR Hadoop v" + hadoopVersion);
            instances.setHadoopVersion(hadoopVersion);
            System.out.println("Using instance count: " + numInstances);
            instances.setInstanceCount(numInstances);
            System.out.println("Using master instance type: " + instanceType);
            instances.setMasterInstanceType(instanceType);
            
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

            System.out.println("\tusing jar URI: " + jobJarS3Location);
            HadoopJarStepConfig jarConfig = new HadoopJarStepConfig(jobJarS3Location);
            System.out.println("\tusing args: " + jobArguments);
            jarConfig.setArgs(jobArguments);
            StepConfig stepConfig =
                new StepConfig(jobJarS3Location.substring(jobJarS3Location.indexOf('/') + 1),
                               jarConfig);
            request.setSteps(Arrays.asList(new StepConfig[] { stepConfig }));
            System.out.println("Configured hadoop jar succesfully!\n");

            //Run the job flow
            RunJobFlowResult result = emrClient.runJobFlow(request);
            System.out.println("Trying to run job flow!\n");
     
            describeClusterRequest.setClusterId(result.getJobFlowId());
            
            
            //Check the status of the running job
            String lastState = "";
            while (true)
            {
            	Thread.sleep(10000);
            	describeClusterResult = emrClient.describeCluster(describeClusterRequest);
            	Cluster cluster = describeClusterResult.getCluster();
            	lastState = cluster.getStatus().getState();
            	System.out.println("Current State of Cluster: " + lastState);
            	if(!lastState.startsWith("TERMINATED")) {
            		continue;
            	}
           // 	s3client.getObject(new GetObjectRequest(jobBucketName, jobID + ".log"), errorLog);
            	if(!lastState.endsWith("ERRORS")) {	
            		
            		s3client.getObject(new GetObjectRequest(jobBucketName, jobID + "_output" + "/part-r-00000"), outputFile);
            		System.out.println("The job has ended and output has been downloaded");
    
            		revLineRead = new ReversedLinesFileReader(outputFile, 4096, Charset.forName("UTF-8"));
            //		System.out.println("First Line: " + revLineRead.readLine());
            		finalAverage = revLineRead.readLine();
            //		System.out.println("Second Line: " + finalAverage.split("\\t")[0]);
            		
            		jsonObj.put(finalAverage.split("\\t")[0], finalAverage.split("\\t")[1]);
            		fileWriter = new FileWriter(outputFile);
            		fileWriter.write(jsonObj.toString() + "\n");
            		fileWriter.flush();
            		fileWriter.close();
            		break;
            	}
            	System.out.println("The job has ended with errors, please check the log");
            	
            	
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
	
}
