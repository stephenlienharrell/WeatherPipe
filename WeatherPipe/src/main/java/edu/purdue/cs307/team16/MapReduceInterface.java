package edu.purdue.cs307.team16;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectSummary;

public abstract class MapReduceInterface {

	protected abstract List<S3ObjectSummary> ListBucket(String bucketName, String key); 
	
	protected abstract String FindOrCreateWeatherPipeJobDirectory();
	
	protected abstract String UploadInputFileList(ArrayList<String> fileList, String dataDirName);
	
	protected abstract String UploadMPJarFile(String fileLocation);
	
	protected abstract void CreateMRJob(String jobInputLocation, String jobJarLocation, int numInstances, String instanceType);
	
}
