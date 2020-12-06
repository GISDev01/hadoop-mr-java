package com.gisdev01.writers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecordWritable implements WritableComparable<RecordWritable> {
    private Logger logger = LogManager.getRootLogger();

    private Text primaryId;
    private Text ipv4;

    public RecordWritable() {
        this.primaryId = new Text();
        this.ipv4 = new Text();
    }

    public RecordWritable(Text primaryId,
                          Text ipv4) {
        this.primaryId = primaryId;
        this.ipv4 = ipv4;
    }

    public void set(Text primaryId,
                    Text ipv4) {
        this.primaryId = primaryId;
        this.ipv4 = ipv4;
    }

    public Text getipv4() {
        return ipv4;
    }

    public Text getPrimaryId() {
        return primaryId;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ipv4.readFields(in);
        primaryId.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        ipv4.write(out);
        primaryId.write(out);
    }

    @Override
    public int compareTo(RecordWritable o) {
        return (primaryId.compareTo(o.primaryId));
    }

    @Override
    public boolean equals(Object o) {
        logger.info("Equals check");
        logger.info(o.toString());
        if (o instanceof RecordWritable) {
            RecordWritable other = (RecordWritable) o;
            logger.info(other.toString());
            logger.info(primaryId.equals(other.primaryId));
            return primaryId.equals(other.primaryId);
            // && ipv4.equals(other.ipv4);
        }
        return false;
    }

    @Override
    public int hashCode() {
        logger.info("Getting hashCode for: "
                + primaryId.toString() + " - "  +
                ipv4.toString());
        return primaryId.hashCode();// + ipv4.hashCode();
    }

    @Override
    public String toString() {
        return primaryId.toString() +
                " - " + ipv4.toString();
    }
}
