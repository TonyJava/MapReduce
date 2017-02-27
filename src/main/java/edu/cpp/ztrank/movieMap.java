package edu.cpp.ztrank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Zachary Rank on 2/26/17.
 */
public class movieMap extends Mapper<LongWritable, Text, LongWritable, DoubleWritable> {

    private LongWritable movie_ID = new LongWritable();
    private DoubleWritable rating = new DoubleWritable();
    private String[] output;

    @Override
    protected void map(LongWritable lineNumber, Text line, Context context) throws IOException, InterruptedException {
        output = line.toString().split(",");
        movie_ID.set(Long.valueOf(output[0]));
        rating.set(Double.valueOf(output[2]));
        context.write(movie_ID, rating);
    }
}
