package edu.cpp.ztrank;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Zachary Rank on 2/26/17.
 */
public class userDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new userDriver(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        boolean ignoreArgs = true;

        if (args.length != 2 && !ignoreArgs) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }

        Job job = new Job();
        job.setJarByClass(userDriver.class);
        job.setJobName("HighestRated");

        FileInputFormat.addInputPath(job, new Path("./a3-dataset/TrainingRatings.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./UserRatings"));

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(userMap.class);
        job.setReducerClass(userReduce.class);

        int returnValue = job.waitForCompletion(true) ? 0 : 1;

        if (job.isSuccessful()) {
            System.out.println("User Job was successful");
        } else if (!job.isSuccessful()) {
            System.out.println("User Job was not successful");
        }

        return returnValue;
    }
}
