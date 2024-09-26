package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GPSErrorCarMapper1 extends Mapper<Object, Text, Text, IntWritable> {

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		// parse the input string
		Utils.ParsedLine parsedLine = new Utils.ParsedLine();
		boolean success = Utils.parseLine(value.toString(), parsedLine);

		if (!success) {
			return;
		}

		context.write(new Text(parsedLine.car), new IntWritable((parsedLine.bad_pickup_pos || parsedLine.bad_dropoff_pos) ? 1 : 0));
	}
}