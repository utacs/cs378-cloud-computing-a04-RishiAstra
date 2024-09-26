package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GPSErrorHourMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		String[] parts = value.toString().split(",");
		if (parts.length != 17) {
			System.out.println("bad line (length): " + value.toString());
			return;
		}
		// dates are like
		// 2013-01-01 00:00:00
		String pickup = parts[2];
		String dropoff = parts[3];
		boolean valid = true;
		if (pickup.length() < 19) valid = false;
		if (dropoff.length() < 19) valid = false;
		if (pickup.charAt(16) != ':' || pickup.charAt(13) != ':' || pickup.charAt(10) != ' ') valid = false;
		if (dropoff.charAt(16) != ':' || dropoff.charAt(13) != ':' || dropoff.charAt(10) != ' ') valid = false;
		if (!valid) {
			System.out.println("bad line (hour format): " + value.toString());
			return;
		}
		float totalCost = 0;
		int tripTimeInSec = 0;
		int pickupHour = 0;
		int dropoffHour = 0;
		try{
			// money checks
			totalCost = Float.parseFloat(parts[16]);
			float cost = 0;
			for (int j = 11; j <= 15; j++) {
				cost += Float.parseFloat(parts[j]);
			}
			if (Math.abs(cost - totalCost) > 0.01) {
				System.out.println("bad line (money): " + value.toString());
				return;
			}

			tripTimeInSec = Integer.parseInt(parts[4]);
			if (tripTimeInSec == 0) {
				System.out.println("bad line (trip time): " + value.toString());
				return;
			}

			pickupHour = Integer.parseInt(pickup.substring(11, 13)) + 1;
			dropoffHour = Integer.parseInt(dropoff.substring(11, 13)) + 1;
		} catch (NumberFormatException e) {
			System.out.println("bad line (int parse): " + value.toString());
			return;
		}
		String car = parts[0];
		String driver = parts[1];

		boolean bad_pickup_pos = false;
		boolean bad_dropoff_pos = false;
		try {
			float pick_long = Float.parseFloat(parts[6]);
			float pick_lat = Float.parseFloat(parts[7]);
			float drop_long = Float.parseFloat(parts[8]);
			float drop_lat = Float.parseFloat(parts[9]);
			bad_pickup_pos = pick_long == 0 || pick_lat == 0;
			bad_dropoff_pos = drop_long == 0 || drop_lat == 0;
		} catch (NumberFormatException e) {
			System.out.println("bad line (float parse): " + value.toString());
			return;
		}

		// gps error per hour entries
		if (bad_pickup_pos) {
			context.write(new IntWritable(pickupHour), counter);
		}
		if (bad_dropoff_pos) {
			context.write(new IntWritable(dropoffHour), counter);
		}
	}
}