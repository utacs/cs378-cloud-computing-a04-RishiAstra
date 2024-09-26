package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgEarnReducer2 extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	PriorityQueue<GPSErrorCar2.Item> pq = new PriorityQueue<GPSErrorCar2.Item>();

	@Override
	public void reduce(Text key, Iterable<FloatWritable> value, Context context)
			throws IOException, InterruptedException {
		for (FloatWritable val : value) {
			pq.add(new GPSErrorCar2.Item(key.toString(), val.get()));

			if (pq.size() > 10) {
				pq.poll();
			}
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