package edu.purdue.cs307.team16;

import java.util.ArrayList;

public abstract class MapReduceInterface {
	
	public String jobOutput;
	public String jobDirName;
	
	protected abstract String FindOrCreateWeatherPipeJobDirectory();
	
	protected abstract String UploadInputFileList(ArrayList<String> fileList, String dataDirName);
	
	protected abstract String UploadMPJarFile(String fileLocation);
	
	protected abstract void CreateMRJob(String jobInputLocation, String jobJarLocation, int numInstances, String instanceType);
	
	protected abstract void close();
}
