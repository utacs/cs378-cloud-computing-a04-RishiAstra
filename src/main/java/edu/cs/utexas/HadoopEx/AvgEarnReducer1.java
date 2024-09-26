package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgEarnReducer1 extends  Reducer<Text, IntFloatWritable, Text, FloatWritable> {

   public void reduce(Text text, Iterable<IntFloatWritable> values, Context context)
           throws IOException, InterruptedException {
	   
         int time = 0;
         int cost = 0;
         
         for (IntFloatWritable value : values) {
            time += value.x;
            cost += value.y;
         }
         context.write(text, new FloatWritable(((float)cost)/((float)time / 60)));
   }
}