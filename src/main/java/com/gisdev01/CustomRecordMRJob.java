package com.gisdev01;

import com.gisdev01.writers.RecordWritable;
import com.gisdev01.util.FileDirUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.File;


public class CustomRecordMRJob extends Configured implements Tool {

    private final int NUM_REDUCE_TASKS = 5;

    // Called from Driver App by the Tool Runner
    public int run(String[] arg0) throws Exception {
        Configuration conf = new Configuration();

        // To run on a hadoop cluster running locally
        // must be used if wanting to use more than
        // 1 reducer for custom partition testing
        conf.set("fs.default.name", "hdfs://localhost:8020");
        conf.set("mapred.job.tracker", "localhost:8021");

        Job job = new Job(conf);

        job.setJarByClass(CustomRecordMRJob.class);
        job.setJobName("recordcustom");

        job.setMapperClass(RecordMapper.class);
        job.setPartitionerClass(RecordPartitioner.class);
        job.setReducerClass(RecordReducer.class);

        job.setMapOutputKeyClass(RecordWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(NUM_REDUCE_TASKS);

        String INPUT_PATH = "/user/gisdev01";
        FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));

        // For local testing (non-HDFS path):
        // remove previous run files or hadoop will complain
        String OUTPUT_PATH = "/user/gisdev01/out";
        File outputDir = new File(OUTPUT_PATH);
        if (outputDir.exists()) {
            FileDirUtils.deleteDir(outputDir);
        }

        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

}
