package com.gisdev01;

import com.gisdev01.writers.RecordWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class RecordReducer extends
        Reducer<RecordWritable, IntWritable,
                Text, IntWritable> {

    private final IntWritable count = new IntWritable();
    private final Logger logger = LogManager.getRootLogger();

    protected void reduce(RecordWritable key,
                          Iterable<IntWritable> values,
                          Context context)
            throws IOException, InterruptedException {

        int sumCount = 0;
        for (IntWritable value : values) {
            sumCount += value.get();
        }
        count.set(sumCount);
        logger.info("Primary ID in Reduce: " + key.getPrimaryId().toString());
        context.write(key.getPrimaryId(), count);
    }

    @Override
    protected void setup(Reducer<RecordWritable, IntWritable,
            Text, IntWritable>.Context context) {
        logger.info("Reducer Setup");
    }

}
