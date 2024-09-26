package edu.cs.utexas.HadoopEx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntFloatWritable implements WritableComparable<IntFloatWritable> {
    public int x;
    public float y;

    public IntFloatWritable() {
        x = 0;
        y = 0;
    }
    
    public IntFloatWritable(int x, float y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        x = arg0.readInt();
        y = arg0.readFloat();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        arg0.writeInt(x);
        arg0.writeFloat(y);
    }

    @Override
    public int compareTo(IntFloatWritable o) {
        if (x == o.x) {
            return Float.compare(y, o.y);
        }
        return x - o.x;
    }

    @Override
    public String toString() {
        return x + "\t" + y;
    }
}
