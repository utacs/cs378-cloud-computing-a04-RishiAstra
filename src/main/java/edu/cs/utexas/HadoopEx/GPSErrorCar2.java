package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class GPSErrorCar2 extends Configured implements Tool {
	public static class Item implements Comparable<Item> {
		String key;
		float value;
		Item(String key, float value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public int compareTo(Item o) {
			return Float.compare(value, o.value);
		}
	}

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */

	/**
	 * 
	 */
	public int run(String args[]) {
		try {
			Configuration conf = new Configuration();

			Job job = new Job(conf, "GPS Errors by Car 2");
			job.setJarByClass(GPSErrorCar2.class);

			// specify a Mapper
			job.setMapperClass(GPSErrorCarMapper2.class);

			// specify a Reducer
			job.setReducerClass(GPSErrorCarReducer2.class);

			// specify output types
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);

			// specify input and output directories
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setOutputFormatClass(TextOutputFormat.class);

			// 1 reducer
			job.setNumReduceTasks(1);

			return (job.waitForCompletion(true) ? 0 : 1);

		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
	}
}
