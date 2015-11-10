package edu.purdue.cs307.team16;

import java.io.Serializable;

public class MapReduceSerializer implements Serializable {

	private static final long serialVersionUID = 3912694648732650934L;
	
	public Object serializeMe;
	
	public MapReduceSerializer(Object serializable) {
		serializeMe = serializable;
	}
	
}