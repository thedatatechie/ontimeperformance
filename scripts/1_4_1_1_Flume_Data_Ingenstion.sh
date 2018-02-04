#!/bin/bash -x


./bin/flume-ng agent --conf conf --conf-file ./data_ingestion_flume_agent.conf --name emirates_agent -Dflume.root.logger=INFO,console