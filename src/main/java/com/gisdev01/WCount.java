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

public class WCount extends Configured implements Tool {

    private String INPUT_PATH = "test/resources/in";
    private String OUTPUT_PATH = "test/resources/out";

    public static class MapClass extends
            Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private Text word = new Text();


        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                word.set(token);
                context.write(word, ONE);
            }
        }
    }

    public static class Reduce extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable count = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context) throws IOException, InterruptedException {

            int sumCount = 0;
            for (IntWritable value : values) {
                sumCount += value.get();
            }
            count.set(sumCount);
            context.write(key, count);
        }
    }

    private void deleteDir(File dir) {
        File[] files = dir.listFiles();

        for (File myFile: files) {
            if (myFile.isDirectory()) {
                deleteDir(myFile);
            }
            myFile.delete();

        }
    }

    public int run(String[] arg0) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(WCount.class);
        job.setJobName("wcounttest");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));



        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

}
