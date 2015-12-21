# WeatherPipe
The goal of this project is to provide an analysis tool for the NEXRAD dataset hosted in S3. The tool uses MapReduce to do the analysis and can be used of fairly large datasets. The tool uses EMR so that it can be easily used by anyone regardless of access to a MapReduce cluster. The tool is designed to abstract away setting up Map Reduce, marshaling of the NEXRAD data into usable data structures, and running the job.

## Instructions for Getting Started
With these steps to get started you can run a simple analysis from the examples. This will help you make sure you have all of the proper permissions and settings before starting to write your own analysis. 

1. Download the WeatherPipe Tool from [here](https://github.com/stephenlienharrell/WeatherPipe/releases) and unzip it
* Download and install the [AWS command line tools](https://aws.amazon.com/cli/)
* Download and install [gradle](http://gradle.org/gradle-download/). Gradle can be unzipped and used as is, but the gradle bin directory must be set in your $PATH or WeatherPipe will not work. 
* Create an [AWS account](http://aws.amazon.com/)
* Run $ [aws configure](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html) to set your AWS credentials.
* Create default EMR roles by running $ aws emr create-default-roles 
* Copy an example analysis from the WeatherPipe/ExampleAnalysis directory to WeatherPipe/WeatherPipeMapReduce/src/main/java/edu/purdue/eaps/weatherpipe/weatherpipemapreduce/ResearcherMapReduceAnalysis.java OR use the one that is provided by default.
* In the WeatherPipe/bin directory you can run ./WeatherPipe -s "01/01/2010 07:30:00" -e "01/01/2010 23:00:00" -st KIND
* Once the job has finished you can find the output in a directory that looks similar to ./WeatherPipeJob2015-12-21T06.07.824 
* If you used the default analysis the output will be in jsonOutputFile
* Start [using Eclipse](#eclipseinstructions) to write your own analysis


<a name="ResearcherMapReduceAnalysis"></a>
## ResearcherMapReduceAnalysis

The [ResearcherMapReduceAnalysis file](https://github.com/stephenlienharrell/WeatherPipe/blob/master/WeatherPipe/src/main/dist/WeatherPipeMapReduce/src/main/java/edu/purdue/eaps/weatherpipe/weatherpipemapreduce/ResearcherMapReduceAnalysis.java) contains the analysis on the NEXRAD data set. After the data has been [marshaled](#marshaling) it is passed as input to the [mapper](#mapping) which runs the mapAnalyze function from the ResearcherMapReduceAnalysis file. The output of the mapAnalyze function is then sent to the reducer which runs the reduceAnalyze function. 

### Data Passing/Generics
The [ResearcherMapReduceAnalysis](#ResearcherMapReduceAnalysis) uses [generics](https://en.wikipedia.org/wiki/Generics_in_Java) to allow the user to pass arbitrary data types between the mapper, reducer and output file writer. Here is an empty class to illustrates the types. I will use TYPEA and TYPEB to illustrate what needs to change with different types. 

    public class ResearcherMapReduceAnalysis extends MapReduceAnalysis<TYPEA, TYPEB> {
	
        public ResearcherMapReduceAnalysis(Configuration conf) {super(conf);}
	
        public ResearcherMapReduceAnalysis() {super(null);}
	
        protected TYPEA mapAnalyze(NetcdfFile nexradNetCDF) {}

        protected TYPEB reduceAnalyze(TYPEA input) {}

        protected void outputFileWriter(TYPEB reduceOutput, String outputDir) {}

TYPEA is defined as the output of the mapper and input of the reducer. TYPEB is defined as the output of the reducer and input to the output file writer. 

TYPEA and TYPEB can only be types that are [serializable](http://www.tutorialspoint.com/java/java_serialization.htm)

### Adding new libraries
You may want to use other libraries in your analysis. In order to add new libraries you must add them to the build.gradle file. There is a stanza that looks like this:

    dependencies {
        compile 'com.amazonaws:aws-java-sdk:1.10.30'
        compile 'org.apache.logging.log4j:log4j-core:2.4'
    }

You can find these maven artifacts on site like [mvnrepo](http://mvnrepository.com/). Just add your maven artifact with the compile directive. Then when you run the tool again the appropriate libraries will be downloaded and wrapped into the MapReduce jar file. If you are using eclipse don't forget to run "gradle eclipse" to rebuild your class path.

<a name="eclipseinstructions"></a>
### Instructions for Setting up Eclipse with WeatherPipe

1. Unzip the WeatherPipe.{zip|tar}
2. cd WeatherPipe/WeatherPipeMapReduce/
3. Run gradle eclipse
4. Open Eclipse
5. File -> Import -> General -> Existing Projects Into Workspace
6. Select WeatherPipe/WeatherPipeMapReduce/ -> Open
7. Click Finish
8. Find ResearcherMapReduceAnalysis
9. Start writing your analysis


## Order of Operations in WeatherPipe

### Compiling MapReduce Jar
WeatherPipe compiles the MapReduce Jar and uploads it. It does this using gradle. The MapReduce jar can be relatively large because all supporting libraries must be packaged inside it.

<a name="marshaling"></a>
### Marshaling the NEXRAD data

This tool searches through the [archive of NEXRAD data on S3](https://aws.amazon.com/noaa-big-data/nexrad/). It looks for specific NEXRAD stations and finds blocks of time at those stations using a start and end time. Once the list of matching NEXRAD files are found, they are uploaded and used as the input for the analysis.

### Running the MapReduce Job
WeatherPipe will read your AWS credentials file, create a new s3 bucket and upload the MapReduce jar and Input file to that bucket. The bucket will also be used for the raw output. Once all files are in place WeatherPipe contacts the EMR web service and starts a job based on the config file/command line parameters you have given. WeatherPipe then monitors the job until it's completion.

<a name="mapping"></a>
### Mapping
WeatherPipe provides an interface so the user does not have to deal with the inputs and outputs using MapReduce. The mapper opens the NEXRAD NETCDF file, does some basic checking to see if it is valid, then passes the open NETCDF file to the [ResearcherMapReduceAnalysis](#ResearcherMapReduceAnalysis). The Mapper then takes the output from that analysis and sends it to the reducer. 

In general as much as possible should be done in the Mapper. The parallelism comes from the Mapper. The reducer is one instance taking in all data from all the Mappers and doing a final analysis it so it is decidedly not parallel. 

<a name="reducing"></a>
### Reducing
The reducer takes output from each mapper. It then instantiates [ResearcherMapReduceAnalysis](#ResearcherMapReduceAnalysis) and runs the reduceAnalyze function in a loop. The significance of this is that you can persist data through each run of the reduceAnalyze function by using class variables in the ResearcherMapReduceAnalysis class. Depending on the analysis this can be less than optimal. For instance in a normal average function you may see something like this:

    int[] elements = {1,2,3}
    int numElements = 0;
    float totalSum = 0;
    for(int element: elements) {
        totalSum += element;
        numElements++;
    }
    float average = totalSum/numElements

In the way WeatherPipe has been implemented you would need to do something like this:

    // elements would be passed one at a time to reduceAnalysis
    // int[] elements = {1,2,3}
    int numElements = 0;
    float totalSum = 0;
    float reduceAnalysis(int element)  {
        totalSum += element;
        numElements++;
        return totalSum/numElements;
    }

The lesson here is that the code is not aware of when the last element has come in. So we must plan for each run to be the last time it is run and give valid output based on whatever data is available now. 

<a name="writingoutput"></a>
### Writing Output

The outputFileWriter function will receive the return value of the **LAST** output of reduceAnalyze. You then can write that out however you would like. Where the mapAnalyze and reduceAnalyze were executed on the cluster, outputFileWriter is executed wherever you are running WeatherPipe and the function gives you the output directory locally where you can write whatever you would like. There are examples for writing both JSON files as well as NETCDF files. Be aware that writing a NETCDF file requires the C NETCDF library to be [installed](https://www.unidata.ucar.edu/software/netcdf/docs/netcdf-install/) on your client machine. NETCDF is provided by [HomeBrew](http://brew.sh/) for Mac and the [EPEL Repo](https://fedoraproject.org/wiki/EPEL) for Fedora/CentOS/RHEL.


## Running WeatherPipe

### Example Command Line
The minimum command line you need to get the analysis running is start_time, end_time and station. 

$ WeatherPipe -s "21/02/2010 07:30:00" -e "21/02/2010 23:00:00" -st KIND

Other flags allow you to set EMR settings and change names of jobs and S3 buckets.

### Analysis Flags/Config Options
Analysis flags deal with the marshaling of NEXRAD data for the analysis, for instance choosing a radar station. The config options match the long name flags.

The flags are:
* -s,--start_time - Start search boundary for NEXRAD data search. Date Format is dd/MM/yyyy HH:mm:ss
* -e,--end_time - End search boundary for NEXRAD data search. Date Format is dd/MM/yyyy HH:mm:ss
* -st,--station - Radar station abbreviation ex. "KIND"

### EMR Flags/Config Options
EMR flags are settings that are used in the running of the MapReduce job

The flags are:

* -b,--bucket_name - Bucket name in S3 to place input and output data. Will be auto-generated if not given
* -i,--instance_count - The amount of instances to run the analysis on. Default is 1.
* -t,--instance_type - Instance type for EMR job. Default is c3.xlarge. See options here: https://aws.amazon.com/elasticmapreduce/pricing/

### Other Flags
These flags are not valid in the config file

* -c,--config_file - Location of config file
* -h,--help - Print help message
* -id,--job_id - Name of this particular job, a random one will be generated if not given. This must be unique in reference to other jobs.


### Config File
A config file is available called WeatherPipe.ini. It can be used to set parameters for the analysis or EMR. It can be loaded with -c or if the config file exists in the current working directory when the job is run. Any command line flags will override parameters in the config file.

### Output Directory and Logs
The output directory will be put in the current working directory. If logs are created they will be downloaded at the successful or unsuccessful termination of the job. There are two relevant directories in regards to the log files:

1. The MapReduce stdout and stderr logs are in logs/localjobid/emrjobid/steps/1/ ex. logs/2015-12-21T06.19.411.log/j-1YSXQE0T0EV49/steps/1/
* The analysis stdout and stderr logs are in logs/localjobid/emrjobid/task-attempts/application-numbers/container-numbers*/ ex. logs/2015-12-21T06.19.411.log/j-1YSXQE0T0EV49/task-attempts/application_1450679007720_0001/container_1450679007720_0001_01_00000*/ 

This directory also contains the job setup files (the input file and MapReduce jar). The raw output file, which is a base64 encoded data structure is also in the directory. This directory is the one passed to the outputDir input parameter for the outputFileWriter function in the ResearchMapReduceAnalysis class. 
