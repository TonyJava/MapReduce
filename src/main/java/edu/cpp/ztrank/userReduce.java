package edu.cpp.ztrank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Zachary Rank on 2/26/17.
 */
public class userReduce extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

    private LongWritable total = new LongWritable();

    public void reduce(LongWritable user_ID, Iterable<LongWritable> tallies, Reducer<LongWritable, LongWritable, LongWritable, LongWritable>.Context context) throws InterruptedException, IOException {
        total.set(0);
        for (LongWritable tally : tallies) total.set(total.get() + tally.get());

        context.write(user_ID, total);
    }
}
