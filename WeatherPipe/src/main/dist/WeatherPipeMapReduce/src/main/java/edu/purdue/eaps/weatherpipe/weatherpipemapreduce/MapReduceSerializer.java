package edu.purdue.eaps.weatherpipe.weatherpipemapreduce;

import java.io.Serializable;

public class MapReduceSerializer implements Serializable {

	private static final long serialVersionUID = 3912694648732650934L;
	
	public Object serializeMe;
	
	public MapReduceSerializer(Object serializable) {
		serializeMe = serializable;
	}
	
}