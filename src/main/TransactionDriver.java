package main;

import analysis.TransactionTypeCountMapper;
import analysis.TransactionTypeCountReducer;
import preprocessing.TransactionPreprocessingMapper;
import preprocessing.TransactionPreprocessingReducer;
import time_analysis.TimeMapper;
import time_analysis.TimeReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TransactionDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) { 
            System.err.println("Usage: TransactionDriver <input path> <intermediate path> <output path> <final output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // First Job - Preprocessing
        Job preprocessingJob = Job.getInstance(conf, "Preprocessing Job");
        preprocessingJob.setJarByClass(TransactionDriver.class);
        preprocessingJob.setMapperClass(TransactionPreprocessingMapper.class);
        preprocessingJob.setReducerClass(TransactionPreprocessingReducer.class);
        preprocessingJob.setOutputKeyClass(Text.class);
        preprocessingJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(preprocessingJob, new Path(args[0])); 
        FileOutputFormat.setOutputPath(preprocessingJob, new Path(args[1])); 

        if (!preprocessingJob.waitForCompletion(true)) {
            System.exit(1);
        }

        // Second Job - Analysis
        Job analysisJob = Job.getInstance(conf, "Analysis Job");
        analysisJob.setJarByClass(TransactionDriver.class);
        analysisJob.setMapperClass(TransactionTypeCountMapper.class);
        analysisJob.setReducerClass(TransactionTypeCountReducer.class);
        analysisJob.setOutputKeyClass(Text.class);
        analysisJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(analysisJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(analysisJob, new Path(args[2])); 

        if (!analysisJob.waitForCompletion(true)) {
            System.exit(1);
        }

        // Third Job - New Job using Preprocessing Job Output
        Job timeAnalysisJob = Job.getInstance(conf, "Analysis Job");
        timeAnalysisJob.setJarByClass(TransactionDriver.class);


        timeAnalysisJob.setMapperClass(TimeMapper.class); 
        timeAnalysisJob.setReducerClass(TimeReducer.class);
        timeAnalysisJob.setOutputKeyClass(Text.class);
        timeAnalysisJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(timeAnalysisJob, new Path(args[1])); 
        FileOutputFormat.setOutputPath(timeAnalysisJob, new Path(args[3])); 

        System.exit(timeAnalysisJob.waitForCompletion(true) ? 0 : 1);
    }
}
