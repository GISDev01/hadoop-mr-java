# hadoop-mr-java
This repo is used to create small-time use cases
that allow for quick comparison between the old HadoopMR way
of doing things vs. the new Spark 3.x way of doing things,
specifically as part of designing large scale data pipelines. 

The near-term idea is to deploy both this job and the spark
job to EMR with the same resources and the same input files (from S3)
and run a few tuning tests to find any performance differences. 
