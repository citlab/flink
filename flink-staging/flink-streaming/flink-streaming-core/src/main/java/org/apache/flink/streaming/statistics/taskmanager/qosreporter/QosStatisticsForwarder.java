/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.statistics.taskmanager.qosreporter;

import akka.actor.ActorRef;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.statistics.StatisticReport;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.statistics.QosStatisticsForwarderFactory;
import org.apache.flink.streaming.statistics.SpecialStatistic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * This class aggregates and forwards stream QoS report data (latencies,
 * throughput, etc) of the tasks of a single job running within the same
 * manager. Each task manager has one instance of this class per job. Qos report
 * data is pre-aggregated for each QoS manager and shipped in a single message
 * to the Qos manager once every {@link #aggregationInterval}. If no QoS data
 * for a Qos manager has been received, messages will be skipped. This class
 * starts its own thread as soon as there is at least on registered task and can
 * be shut down by invoking {@link #shutdown()}.
 *
 * This class is threadsafe.
 *
 * @author Bjoern Lohrmann
 * @author Sascha Wolke
 */
public class QosStatisticsForwarder extends Thread {
	public static final String FORWARDER_REPORT_INTERVAL_KEY = "qosStatisticsReportInterval"; // used in job config
	public static final long DEFAULT_FORWARD_REPORT_INTERVAL = 7000;

	private static final Logger LOG = LoggerFactory.getLogger(QosStatisticsForwarder.class);

	private final JobID jobId;
	private final ActorRef jobManager;
	private final long aggregationInterval;
	private boolean isRunning = false;
	private boolean isShuttingDown = false;

	private Set<Integer> runningInstances = new HashSet<Integer>();

	public QosStatisticsForwarder(JobID jobId, ActorRef jobManager, Configuration jobConfiguration) {
		this.jobId = jobId;
		this.jobManager = jobManager;
		this.aggregationInterval = jobConfiguration
				.getLong(FORWARDER_REPORT_INTERVAL_KEY, DEFAULT_FORWARD_REPORT_INTERVAL);
		setName(String.format("QosReporterForwarderThread (JobID: %s)", jobId.toString()));
	}

	@Override
	public void run() {
		LOG.info("Forwarder on job {} started.", this.jobId);

		try {
			while(!interrupted() && !this.isShuttingDown) {
				LOG.info("Report...");
				jobManager.tell(new StatisticReport(jobId, new SpecialStatistic("test")), ActorRef.noSender());

				Thread.sleep(this.aggregationInterval);
			}
		} catch(InterruptedException e) {
		}
	}

	private void ensureIsRunning() {
		if (!this.isRunning) {
			start();
			this.isRunning = true;
		}
	}

	private void shutdown() {
		LOG.info("Forwarder on job {} finished.", this.jobId);
		this.isShuttingDown = true;
		interrupt();
		QosStatisticsForwarderFactory.removeForwarderInstance(this.jobId);
	}

	public synchronized void registerTask(StreamTask vertex) {
		this.runningInstances.add(vertex.getInstanceID());
		LOG.info("{} running operators after opening operator {}.", runningInstances.size(), vertex.getInstanceID());
		ensureIsRunning();
	}

	public synchronized void unregisterTask(StreamTask vertex) {
		this.runningInstances.remove(vertex.getInstanceID());
		LOG.info("{} running operators left after closing operator {}. Empty = ", runningInstances.size(), vertex.getInstanceID());

		if (this.runningInstances.isEmpty()) {
			shutdown();
		}
	}
}
