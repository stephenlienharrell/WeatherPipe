import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {	

    public void reduce(Text keyname, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        

        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        try {
        	context.write(keyname, new IntWritable(sum));
        }
        catch (Exception E) {
        	// Send signal
        }
    }
}