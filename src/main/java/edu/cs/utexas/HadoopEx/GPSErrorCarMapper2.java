package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GPSErrorCarMapper2 extends Mapper<Object, Text, Text, FloatWritable> {
	// private PriorityQueue<Float> pq = new PriorityQueue<Float>();
	private PriorityQueue<GPSErrorCar2.Item> pq = new PriorityQueue<GPSErrorCar2.Item>();

	@Override
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] parts = value.toString().split("\t");
		String car = parts[0];
		float error = Float.parseFloat(parts[1]);
		pq.add(new GPSErrorCar2.Item(car, error));

		if (pq.size() > 5) {
			pq.poll();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		while (!pq.isEmpty()) {
			GPSErrorCar2.Item item = pq.poll();
			context.write(new Text(item.key), new FloatWritable(item.value));
		}
	}
}