package com.gisdev01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class DriverApp {

    public static void main(String[] args) throws Exception {
        int response = ToolRunner.run(new Configuration(),
                new WCount(),
                args);

        System.exit(response);
    }
}
