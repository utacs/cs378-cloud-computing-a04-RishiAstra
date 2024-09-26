package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GPSErrorCarReducer1 extends  Reducer<Text, IntWritable, Text, FloatWritable> {

   public void reduce(Text text, Iterable<IntWritable> values, Context context)
           throws IOException, InterruptedException {
	   
         int bad = 0;
         int total = 0;
         
         for (IntWritable value : values) {
            total++;
            bad += value.get();
         }
         context.write(text, new FloatWritable(((float)bad)/total));
   }
}