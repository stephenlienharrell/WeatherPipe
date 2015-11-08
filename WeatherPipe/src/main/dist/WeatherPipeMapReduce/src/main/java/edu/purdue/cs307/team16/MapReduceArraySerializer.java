package edu.purdue.cs307.team16;
import java.io.Serializable;

public class MapReduceArraySerializer implements Serializable {
	private static final long serialVersionUID = 7526471155622776147L;
	
		public int[] numbers;
		public MapReduceArraySerializer(int[] array) {
			numbers = array;
		}
}
