package edu.cpp.ztrank;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by Zachary Rank on 2/28/17.
 */
public class jobsDriver extends Configured implements Tool {
//    public static void main(String[] args) throws Exception {
//        int exitCode = ToolRunner.run(new jobsDriver(), args);
//        System.exit(exitCode);
//    }

    public int run(String[] args) throws Exception {
        boolean ignoreArgs = true;

        if (args.length != 2 && !ignoreArgs) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }

        int movie = movieJob();
        int user = userJob();

        if (movie == 0 && user == 0) {
            return 0;
        } else return 1;
    }

    public int movieJob() throws IOException, ClassNotFoundException, InterruptedException {

        Job job = new Job();
        job.setJarByClass(jobsDriver.class);
        job.setJobName("HighestRated");

        FileInputFormat.addInputPath(job, new Path("./a3-dataset/TrainingRatings.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./MovieRatings"));

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(movieMap.class);
        job.setReducerClass(movieReduce.class);

        int returnValue = job.waitForCompletion(true) ? 0 : 1;

        if (job.isSuccessful()) {
            System.out.println("Movie Job was successful");
        } else if (!job.isSuccessful()) {
            System.out.println("Movie Job was not successful");
        }

        return returnValue;
    }

    public int userJob() throws IOException, ClassNotFoundException, InterruptedException {

        Job job = new Job();
        job.setJarByClass(jobsDriver.class);
        job.setJobName("MostReviews");

        FileInputFormat.addInputPath(job, new Path("./a3-dataset/TrainingRatings.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./UserReviews"));

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
