/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.statistics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.messages.ExecutionGraphMessages.ExecutionStateChanged;
import org.apache.flink.runtime.messages.ExecutionGraphMessages.JobStatusChanged;
import org.apache.flink.runtime.statistics.AbstractCentralStatisticsHandler;
import org.apache.flink.runtime.statistics.CustomStatistic;
import org.apache.flink.streaming.statistics.jobmanager.autoscaling.ElasticTaskQosAutoScalingThread;
import org.apache.flink.streaming.statistics.message.qosreport.QosReport;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.QosManagerThread;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.QosModel;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CentralQosStatisticsHandler extends AbstractCentralStatisticsHandler {
	private static final Logger LOG = LoggerFactory.getLogger(CentralQosStatisticsHandler.class);
	private JobID jobID;
	private ExecutionGraph executionGraph;

	private QosGraph qosGraph;
	private QosModel qosModel;
	private ElasticTaskQosAutoScalingThread autoScalingThread;
	private QosManagerThread qosManagerThread;

	@Override
	public void open(JobID jobID, ExecutionGraph executionGraph) {
		this.jobID = jobID;
		this.executionGraph = executionGraph;

		this.qosGraph = QosGraph.buildQosGraphFromJobConfig(executionGraph);
		this.qosModel = new QosModel(this.qosGraph);
		this.autoScalingThread = new ElasticTaskQosAutoScalingThread(executionGraph, this.qosGraph);
		this.autoScalingThread.start();
		this.qosManagerThread = new QosManagerThread(jobID, this.qosModel, this.autoScalingThread);
		this.qosManagerThread.start();

		// TODO: merge auto scaling with manager thread (see QosSetupManager)

		LOG.info("New QoS statistics handler launched!");
	}

	@Override
	public void handleStatistic(CustomStatistic statistic) {
		LOG.debug("Statistics received: {}", statistic.getClass().getSimpleName());

		if (statistic instanceof QosReport) {
			this.qosManagerThread.enqueueMessage((QosReport) statistic);

		} else {
			LOG.error("Got unknown statistic: {}.", statistic.getClass().getSimpleName());
		}
	}

	@Override
	public void handleExecutionStateChanged(ExecutionStateChanged executionStatus) {
		LOG.debug("Execution state change received: {}", executionStatus);
		Execution execution = executionGraph.getRegisteredExecutions().get(executionStatus.executionID());

		if (execution != null) {
			this.qosModel.handOffVertexStatusChange(
					executionStatus.newExecutionState(), execution.getVertex()
			);
		}
	}

	@Override
	public void handleJobStatusChanged(JobStatusChanged jobStatus) {
		LOG.debug("Job status change received: {}", jobStatus);
	}

	@Override
	public void close() {
		LOG.debug("Going to close central QoS statistics handler.");
		this.qosManagerThread.shutdown();
		this.autoScalingThread.shutdown();
		LOG.info("Central Qos statistics handler closed!");
	}
}
