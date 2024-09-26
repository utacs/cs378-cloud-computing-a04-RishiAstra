package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgEarnMapper1 extends Mapper<Object, Text, Text, IntFloatWritable> {

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		// parse the input string
		Utils.ParsedLine parsedLine = new Utils.ParsedLine();
		boolean success = Utils.parseLine(value.toString(), parsedLine);
		if (!success) {
			return;
		}

		// driver time and total cost
		context.write(new Text(parsedLine.driver), new IntFloatWritable(parsedLine.tripTimeInSec, parsedLine.totalCost));
	}
}