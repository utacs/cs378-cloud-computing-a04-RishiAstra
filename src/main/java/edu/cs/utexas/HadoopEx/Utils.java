package edu.cs.utexas.HadoopEx;


public class Utils {
	public static final boolean PRINT_BAD_LINES = false;

	public static class ParsedLine {
		public float totalCost;
		public int tripTimeInSec;
		public int pickupHour;
		public int dropoffHour;
		public String car;
		public String driver;
		public boolean bad_pickup_pos;
		public boolean bad_dropoff_pos;
	}
	public static void printBadLine(String line) {
		if (PRINT_BAD_LINES) {
			System.out.println("bad line: " + line);
		}
	}
	public static boolean parseLine(String line, ParsedLine out) {
		String[] parts = line.split(",");
		if (parts.length != 17) {
			printBadLine("bad line (length): " + line);
			return false;
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
			printBadLine("bad line (hour format): " + line);
			return false;
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
				printBadLine("bad line (money): " + line);
				return false;
			}

			tripTimeInSec = Integer.parseInt(parts[4]);
			if (tripTimeInSec == 0) {
				printBadLine("bad line (trip time): " + line);
				return false;
			}

			pickupHour = Integer.parseInt(pickup.substring(11, 13)) + 1;
			dropoffHour = Integer.parseInt(dropoff.substring(11, 13)) + 1;
		} catch (NumberFormatException e) {
			printBadLine("bad line (int parse): " + line);
			return false;
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
			printBadLine("bad line (float parse): " + line);
			return false;
		}

		out.totalCost = totalCost;
		out.tripTimeInSec = tripTimeInSec;
		out.pickupHour = pickupHour;
		out.dropoffHour = dropoffHour;
		out.car = car;
		out.driver = driver;
		out.bad_pickup_pos = bad_pickup_pos;
		out.bad_dropoff_pos = bad_dropoff_pos;

		return true;
	}
}
