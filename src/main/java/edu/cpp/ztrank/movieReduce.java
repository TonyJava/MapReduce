package edu.cpp.ztrank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Zachary Rank on 2/26/17.
 */
public class movieReduce extends Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {

    private DoubleWritable average = new DoubleWritable();
    private double sum;
    private long count;

    @Override
    protected void reduce(LongWritable movie_ID, Iterable<DoubleWritable> ratings, Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
        sum = 0;
        count = 0;
        for (DoubleWritable rating : ratings) {
            sum += rating.get();
            count++;
        }

        average.set(sum / count);
        context.write(movie_ID, average);
    }
}
