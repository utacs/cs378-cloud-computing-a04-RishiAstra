package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GPSErrorHourMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		// parse the input string
		Utils.ParsedLine parsedLine = new Utils.ParsedLine();
		boolean success = Utils.parseLine(value.toString(), parsedLine);

		if (!success) {
			return;
		}

		// gps error per hour entries
		if (parsedLine.bad_pickup_pos) {
			context.write(new IntWritable(parsedLine.pickupHour), counter);
		}
		if (parsedLine.bad_dropoff_pos) {
			context.write(new IntWritable(parsedLine.dropoffHour), counter);
		}
	}
}