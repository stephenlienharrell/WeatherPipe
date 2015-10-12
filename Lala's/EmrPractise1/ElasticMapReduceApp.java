import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.io.IOException;
import java.lang.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
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

/**
 * Run the Amazon Cloudburst example directly using the AWS SDK for Java.
 *
 * @author mpouttuclarke
 *
 */
public class ElasticMapReduceApp
{

    private static final String HADOOP_VERSION = "0.20";
    private static final int INSTANCE_COUNT = 1;
    private static final String INSTANCE_TYPE = InstanceType.M1Large.toString();
    private static final UUID RANDOM_UUID = UUID.randomUUID();
    private static final String FLOW_NAME = "cloudburst-" + RANDOM_UUID.toString();
    private static final String BUCKET_NAME = "emr-is-for-noobs";
    private static final String S3N_HADOOP_JAR = "s3n://elasticmapreduce/samples/cloudburst/cloudburst.jar";
    private static final String S3N_LOG_URI = "s3n://" + BUCKET_NAME + "/";
    private static final String[] JOB_ARGS = new String[] { "s3n://elasticmapreduce/samples/cloudburst/input/s_suis.br",
                      "s3n://elasticmapreduce/samples/cloudburst/input/100k.br",
                      "s3n://" + BUCKET_NAME + "/" + FLOW_NAME, "36", "3", "0",
                      "1",
                      "240",
                      "48",
                      "24", "24", "128", "16" };
    private static final List<String> ARGS_AS_LIST = Arrays.asList(JOB_ARGS);
    private static final List<JobFlowExecutionState> DONE_STATES = Arrays.asList(new JobFlowExecutionState[] { JobFlowExecutionState.COMPLETED,
                                             JobFlowExecutionState.FAILED,
                                             JobFlowExecutionState.TERMINATED });
    static AmazonElasticMapReduce emr;

    /**
     * The only information needed to create a client are security credentials consisting of the AWS
     * Access Key ID and Secret Access Key. All other configuration, such as the service end points,
     * are performed automatically. Client parameters, such as proxies, can be specified in an
     * optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.PropertiesCredentials
     * @see com.amazonaws.ClientConfiguration
     */
    private static void init() throws Exception {
    	AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/Users/lalavaishnode/.aws/credentials), and is in valid format.",
                    e);
        }
        emr = new AmazonElasticMapReduceClient(credentials);
        System.out.println("WTF IS WRONG\n");
    }

    public static void main(String[] args) throws Exception {

        System.out.println("===========================================");
        System.out.println("Welcome to the Elastic Map Reduce!");
        System.out.println("===========================================");
        System.out.println("calling init");
        init();

        try {
            // Configure instances to use
            JobFlowInstancesConfig instances = new JobFlowInstancesConfig();
            System.out.println("Using EMR Hadoop v" + HADOOP_VERSION);
            instances.setHadoopVersion(HADOOP_VERSION);
            System.out.println("Using instance count: " + INSTANCE_COUNT);
            instances.setInstanceCount(INSTANCE_COUNT);
            System.out.println("Using master instance type: " + INSTANCE_TYPE);
            instances.setMasterInstanceType(INSTANCE_TYPE);
            System.out.println("Using slave instance type: " + INSTANCE_TYPE);
            instances.setSlaveInstanceType(INSTANCE_TYPE);

            // Configure the job flow
            System.out.println("Configuring flow: " + FLOW_NAME);
            RunJobFlowRequest request = new RunJobFlowRequest(FLOW_NAME, instances);
            System.out.println("\tusing log URI: " + S3N_LOG_URI);
            request.setLogUri(S3N_LOG_URI);

            // Configure the Hadoop jar to use
            System.out.println("\tusing jar URI: " + S3N_HADOOP_JAR);
            HadoopJarStepConfig jarConfig = new HadoopJarStepConfig(S3N_HADOOP_JAR);
            System.out.println("\tusing args: " + ARGS_AS_LIST);
            jarConfig.setArgs(ARGS_AS_LIST);
            StepConfig stepConfig =
                new StepConfig(S3N_HADOOP_JAR.substring(S3N_HADOOP_JAR.indexOf('/') + 1),
                               jarConfig);
            request.setSteps(Arrays.asList(new StepConfig[] { stepConfig }));
            System.out.println("Configured hadoop jar succesfully!\n");

            //Run the job flow
            RunJobFlowResult result = emr.runJobFlow(request);
            System.out.println("Trying to run job flow!\n");
            
            //Check the status of the running job
            String lastState = "";
            STATUS_LOOP: while (true)
            {
                DescribeJobFlowsRequest desc =
                    new DescribeJobFlowsRequest(
                                                Arrays.asList(new String[] { result.getJobFlowId() }));
                DescribeJobFlowsResult descResult = emr.describeJobFlows(desc);
                for (JobFlowDetail detail : descResult.getJobFlows())
                {
                    String state = detail.getExecutionStatusDetail().getState();
                    if (isDone(state))
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
        }
    }

    /**
     * @param state
     * @return
     */
    public static boolean isDone(String value)
    {
        JobFlowExecutionState state = JobFlowExecutionState.fromValue(value);
        return DONE_STATES.contains(state);
    }
}