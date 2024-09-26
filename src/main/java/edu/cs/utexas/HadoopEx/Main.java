package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Main {
    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new GPSErrorHour(), args);
		System.exit(res);
	}
}
