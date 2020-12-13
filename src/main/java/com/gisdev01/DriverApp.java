package com.gisdev01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;


public class DriverApp {

    public static void main(String[] args) throws Exception {
        Configurator.setRootLevel(Level.INFO);
        Logger logger = LogManager.getLogger("WCountMR");

        int response = ToolRunner.run(new Configuration(),
                new CustomRecordMRJob(),
                args);
        logger.info("Tool Response Status Code: " + response);
        System.exit(response);
    }
}
