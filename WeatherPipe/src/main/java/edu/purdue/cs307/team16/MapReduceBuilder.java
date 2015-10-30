package edu.purdue.cs307.team16;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;


public class MapReduceBuilder {
	
	FileSystem fs = FileSystems.getDefault();
	Path gradleBinary = fs.getPath("/opt/local/bin/gradle");

	
	
	boolean CheckGradleCompat() {
		return true;
	}
	
	File findGradleJars() {
		Path realPath = null;
		Path libPath;
		String gradleRootPath;
		Path gradleLibPath;
		try {
			realPath = gradleBinary.toRealPath();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		realPath = realPath.normalize();
		libPath = realPath.getParent();
		gradleRootPath = libPath.subpath(0, libPath.getNameCount() - 1).toString();

		return new File("thing");
		
		
		
	}
	
	File findMapReduceBuildPath() {
		 String path = WeatherPipe.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		 String decodedPath;
		 
		 try {
			decodedPath = URLDecoder.decode(path, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		return new File("");
	}

}
