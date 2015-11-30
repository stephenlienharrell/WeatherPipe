# WeatherPipe

The goal of this project is to provide an analysis tool for the [NEXRAD dataset hosted in S3](https://aws.amazon.com/noaa-big-data/nexrad/). The tool uses MapReduce to do the analysis and can be used of fairly large datasets. For the first implementation we have used [EMR](https://aws.amazon.com/elasticmapreduce/) so that it can be easily used by anyone regardless of access to a MapReduce cluster. 

The tool is designed to abstract away setting up Map Reduce, marshalling of the NEXRAD data into usable data structures, and running the job. The only file that the researcher should modify is [this](https://github.com/stephenlienharrell/CS307Team16/blob/master/WeatherPipe/src/main/dist/WeatherPipeMapReduce/src/main/java/edu/purdue/cs307/team16/ResearcherMapReduceAnalysis.java). In this file there are three functions that must be filled out, mapAnalyze, reduceAnalyze and outputFileWriter. These functions will be run in the normal MapReduce workflow and researchers are able to use arbitrary types between the functions with some [caveats](http://www.tutorialspoint.com/java/java_serialization.htm).


The tool is command line and is known to work on Mac and Linux. 
It requires the [aws command line tools](https://aws.amazon.com/cli/) and [gradle](http://gradle.org/gradle-download/) to work.

In order to use the tool you will need an AWS account with a [proper credentials file](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html).

You will also need to create the default EMR Profile: ```aws emr create-default-roles```

To compile the beta code (we will improve this in the future):
```
cd WeatherPipe
gradle build
cd build/distributions
tar xfv WeatherPipe.tar
# Then as an example to run with NEXRAD files between Feb 21, 2010 at 7:30am through Feb 23rd, 2010 at 11:00pm using the KIND radar station.
WeatherPipe/bin/WeatherPipe -s "21/02/2010 07:30:00" -e "23/02/2010 23:00:00" -st KIND

```





