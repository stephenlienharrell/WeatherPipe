package edu.purdue.cs307.team16;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowDetail;
import com.amazonaws.services.elasticmapreduce.model.JobFlowExecutionState;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GetBucketLocationRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;


public class AwsHelpers {


	private String jobBucketName = "weatherpipe";
	private String jobID;
	private AmazonElasticMapReduce emrClient;
	private AmazonS3 s3client;
	
	public AwsHelpers(String job){
		AwsBootstrap(job);
	}
	
	public AwsHelpers(String job, String bucket){
		AwsBootstrap(job);
		jobBucketName = bucket;
	}
	
	private void AwsBootstrap(String job) {
		AWSCredentials credentials;
		Region region;
		
		credentials = new ProfileCredentialsProvider().getCredentials();
		// add better credential searching later
		
		region = Region.getRegion(Regions.US_EAST_1);
	    s3client = new AmazonS3Client(credentials);
		s3client.setRegion(region);
		
		emrClient = new AmazonElasticMapReduceClient(credentials);
		emrClient.setRegion(region);
		
		jobID = job;
	}
	
	public List<S3ObjectSummary> ListBucket(String bucketName, String key) {
		
		ObjectListing listing = s3client.listObjects( bucketName, key );
		List<S3ObjectSummary> summaries = listing.getObjectSummaries();

		while (listing.isTruncated()) {
		   listing = s3client.listNextBatchOfObjects (listing);
		   summaries.addAll (listing.getObjectSummaries());
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
            // Get location.
            bucketLocation = s3client.getBucketLocation(
            		new GetBucketLocationRequest(jobBucketName));
            
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
         } catch (AmazonClientException ace) {
             System.out.println("Caught an AmazonClientException, which " +
             		"means the client encountered " +
                     "an internal error while trying to " +
                     "communicate with S3, " +
                     "such as not being able to access the network.");
             System.out.println("Error Message: " + ace.getMessage());
         
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
		s3client.putObject(new PutObjectRequest(jobBucketName, key, uploadFileStream, objMeta));		
		
		return "s3n://" + jobBucketName + "/" + key;
	}
	
	public String UploadMPJarFile(String fileLocation) {
		String key = jobID + "WeatherPipeMapreduce.jar";
		s3client.putObject(new PutObjectRequest(jobBucketName, key, fileLocation));
		
		return "s3n://" + jobBucketName + "/" + key;
	}

	public void CreateEMRJob(String jobInputS3Location, String jobJarS3Location, int numInstances, String instanceType) {
		
		// Modified from https://mpouttuclarke.wordpress.com/2011/06/24/how-to-run-an-elastic-mapreduce-job-using-the-java-sdk/
		
		String hadoopVersion = "2.6.0";
		String flowName = "WeatherPipe_" + jobID;
		String logS3Location = "s3n://" + jobBucketName+ "/" + jobID + ".log";
		String[] arguments = new String[] {jobInputS3Location};
		List<String> jobArguments = Arrays.asList(arguments);
		
		List<JobFlowExecutionState> DONE_STATES = Arrays.asList(new JobFlowExecutionState[] { JobFlowExecutionState.COMPLETED,
                JobFlowExecutionState.FAILED,
                JobFlowExecutionState.TERMINATED });
		
        try {
            // Configure instances to use
            JobFlowInstancesConfig instances = new JobFlowInstancesConfig();
//            System.out.println("Using EMR Hadoop v" + hadoopVersion);
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
            
            
      
            request.setJobFlowRole("EMRJobflowDefault");

            // Configure the Hadoop jar to use
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
            
            //Check the status of the running job
            String lastState = "";
            STATUS_LOOP: while (true)
            {
                DescribeJobFlowsRequest desc =
                    new DescribeJobFlowsRequest(
                                                Arrays.asList(new String[] { result.getJobFlowId() }));
                DescribeJobFlowsResult descResult = emrClient.describeJobFlows(desc);
                for (JobFlowDetail detail : descResult.getJobFlows())
                {
                    String state = detail.getExecutionStatusDetail().getState();
                    JobFlowExecutionState detailState = JobFlowExecutionState.fromValue(state);
                    if (DONE_STATES.contains(detailState))
                    {
                        System.out.println("Job " + state + ": " + detail.toString());
                        break STATUS_LOOP;
                    }
                    else if (!lastState.equals(state))
                    {
                        lastState = state;
                        System.out.println("Job " + state + " at " + new Date().toString());
                    }
                }
                Thread.sleep(10000);
            }
        } catch (AmazonServiceException ase) {
                System.out.println("Caught Exception: " + ase.getMessage());
                System.out.println("Reponse Status Code: " + ase.getStatusCode());
                System.out.println("Error Code: " + ase.getErrorCode());
                System.out.println("Request ID: " + ase.getRequestId());
        } catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
    }
	
}