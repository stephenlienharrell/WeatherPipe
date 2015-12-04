package edu.purdue.cs307.team16;
 
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
 
import sun.tools.jar.Main;
 
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
 
 
 
public class WeatherPipeFileWriter {
 
    void writeOutput(String jobOutput, String outputDir, String mapReduceJarFileLocation) throws MalformedURLException, ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    	
    	if(jobOutput.startsWith("FAIL")) return;
    	
        // UGLY DYNAMIC LOADING OF MAPREDUCE JAR TO GET DATA TYPES
        // OH GOD, IT BUUUUUURNS
        URLClassLoader child = new URLClassLoader (new URL[] {new URL("file://" + mapReduceJarFileLocation)}, 
                Main.class.getClassLoader());
        @SuppressWarnings("rawtypes")
        Class classToLoad = Class.forName("edu.purdue.cs307.team16.ResearcherMapReduceAnalysis", true, child);
        @SuppressWarnings("unchecked")
        Method method = classToLoad.getMethod ("writeFile",
        		new Class<?>[] {String.class, String.class});
        Object instance = classToLoad.newInstance();
        method.invoke(instance, new Object[] {jobOutput, outputDir});
     
    }
}

