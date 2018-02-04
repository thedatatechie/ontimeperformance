#!/bin/bash -x

export SPARK_MAJOR_VERSION=2

spark-submit \
--master yarn \
--name parquet_converter \
--num-executors 4 \
--deploy-mode client \
--conf "spark.driver.cores=4" \
--conf "spark.default.parallelism=512" \
--conf "spark.sql.shuffle.partitions=512" \
--conf "spark.sql.files.maxPartitionBytes=268435456" \
--conf "spark.executor.memory=1G" \
--conf "spark.driver.memory=4G" \
--conf "spark.driver.maxResultSize=2G" \
--conf "spark.executor.cores=4" \
--conf "spark.yarn.executor.memoryOverhead=2G" \
--conf "spark.eventLog.enabled=true" \
--conf "spark.eventLog.compress=true" \
--conf "spark.eventLog.dir=/tmp/data/spark-events" \
--conf "spark.ui.retainedJobs=3000" \
--conf "spark.ui.retainedStages=3000" \
--conf "spark.ui.retainedTasks=300000" \
--conf "spark.worker.ui.retainedExecutors=3000" \
--conf "spark.worker.ui.retainedDrivers=3000" \
--conf "spark.sql.ui.retainedExecutions=3000" \
--conf "spark.streaming.ui.retainedBatches=3000" \
--class com.emirates.analytics.Main \
sparkapp-1.0-SNAPSHOT-jar-with-dependencies.jar \
--airports-path "/user/vijaya/data/modelled/airports" \
--carriers-path "/user/vijaya/data/modelled/carriers" \
--planedate-path "/user/vijaya/data/modelled/planedate" \
--otp-path "/user/vijaya/data/modelled/otp" > spark_query_processor.log 2>&1 &
