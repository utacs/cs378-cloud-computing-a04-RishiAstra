package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Main {
    public static void main(String[] orig_args) throws Exception {
        if (orig_args.length != 2) {
            System.err.println("Usage: Main <input file> <output prefix>");
        }

        int res = 0;
        String[] args = new String[2];
        
        args[0] = orig_args[0];
        args[1] = orig_args[1] + "/gps_error";
		res = ToolRunner.run(new Configuration(), new GPSErrorHour(), args);
        System.out.println("Result: " + res);
        if (res != 0) {
            System.exit(res);
        }

        args[0] = orig_args[0];
        args[1] = orig_args[1] + "/gps_error_car_intermediate";
        res = ToolRunner.run(new Configuration(), new GPSErrorCar1(), args);
        if (res != 0) {
            System.exit(res);
        }
        args[0] = orig_args[1] + "/gps_error_car_intermediate";
        args[1] = orig_args[1] + "/gps_error_car";
        res = ToolRunner.run(new Configuration(), new GPSErrorCar2(), args);
        System.out.println("Result: " + res);
        if (res != 0) {
            System.exit(res);
        }

        args[0] = orig_args[0];
        args[1] = orig_args[1] + "/avg_earn_intermediate";
        res = ToolRunner.run(new Configuration(), new AvgEarn1(), args);
        if (res != 0) {
            System.exit(res);
        }
        args[0] = orig_args[1] + "/avg_earn_intermediate";
        args[1] = orig_args[1] + "/avg_earn";
        res = ToolRunner.run(new Configuration(), new AvgEarn2(), args);
        System.out.println("Result: " + res);
        System.exit(res);
	}
}
