package com.gisdev01;

import com.gisdev01.writers.RecordWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RecordPartitioner extends
        Partitioner<RecordWritable, IntWritable> {
    private final Logger logger = LogManager.getRootLogger();

    @Override
    public int getPartition(RecordWritable recordWritable, IntWritable intWritable,
                            int numReduceTasks) {
        logger.info("Getting Partition");
        String primaryId = recordWritable.getPrimaryId().toString();

        if (numReduceTasks == 0) {
            logger.warn("No Reduce Tasks configured.");
            return 0;
        }

        if (primaryId.equals("ID0001")) {
            return 1;
        } else if (primaryId.equals("ID0002")) {
            return 2;
        } else {
            return 3;
        }
    }
}

