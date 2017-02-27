package edu.cpp.ztrank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Zachary Rank on 2/26/17.
 */
public class userMap extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

    private LongWritable user_ID = new LongWritable();
    private final static LongWritable tally = new LongWritable(1);
    private String[] output;

    @Override
    protected void map(LongWritable lineNumber, Text line, Context context) throws IOException, InterruptedException {
        output = line.toString().split(",");
        user_ID.set(Long.valueOf(output[1]));
        context.write(user_ID, tally);
    }
}
