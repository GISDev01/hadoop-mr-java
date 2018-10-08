package com.gisdev01;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.File;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;


public class WCount extends Configured implements Tool {

    private String INPUT_PATH = "test/resources/in";
    private String OUTPUT_PATH = "test/resources/out";
    private Logger logger = LogManager.getRootLogger();

    private static Boolean SINGLE_WORD_FILTER_BOOL = true;
    private static String SINGLE_WORD_FILTER = "specialword4";


    public static class TokenizeMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private Text word = new Text();
        private Logger logger = LogManager.getRootLogger();
        private Integer lineCount = 1;

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            //tokenize each line within each file detected in the input directory
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                logger.debug("Mapper found token: " + token);
                if (SINGLE_WORD_FILTER_BOOL) {
                    if (SINGLE_WORD_FILTER.equals(token)) {
                        word.set(token);
                        context.write(word, ONE);
                    }
                }
                else {
                    word.set(token);
                    context.write(word, ONE);
                }


            }
            logger.info("Mapper: line {}, read complete.", lineCount);

            lineCount++;
        }
    }

    public static class CountSumReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable count = new IntWritable();
        private Logger logger = LogManager.getRootLogger();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context) throws IOException, InterruptedException {

            int sumCount = 0;
            for (IntWritable value : values) {
                sumCount += value.get();
            }
            count.set(sumCount);
            context.write(key, count);

            logger.info("CountSumReducer Complete");

        }
    }

    /*
    Remove old data from the hadoop output directory before the next tool run
     */
    private void deleteDir(File dir) {
        logger.info("Deleting directory: " + dir.toString());
        File[] files = dir.listFiles();

        if (files != null) {
            for (File existingFile: files) {
                if (existingFile.isDirectory()) {
                    deleteDir(existingFile);
                }
                existingFile.delete();
            }
        }
        dir.delete();
    }

    /*
    Called from Driver App by the Tool Runner
     */
    public int run(String[] arg0) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(WCount.class);
        job.setJobName("wcounttest");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TokenizeMapper.class);
        job.setReducerClass(CountSumReducer.class);

        FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));

        // For local testing (non-HDFS path): remove previous run files or hadoop will complain
        File outputDir = new File(OUTPUT_PATH);
        if (outputDir.exists()) deleteDir(outputDir);

        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        boolean success = job.waitForCompletion(true);
        logger.info("MR Job Success: " + success);


        return success ? 0 : 1;
    }

}
