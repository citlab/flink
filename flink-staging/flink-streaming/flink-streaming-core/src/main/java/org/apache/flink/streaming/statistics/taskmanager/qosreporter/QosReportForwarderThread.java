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
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.statistics.StatisticReport;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.statistics.SpecialStatistic;
import org.apache.flink.streaming.statistics.message.qosreport.AbstractQosReportRecord;
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
// TODO: handle start/stop and final cleanup
public class QosReportForwarderThread extends Thread {
	public static final String FORWARDER_REPORT_INTERVAL_KEY = "qosStatisticsReportInterval"; // used in job config
	public static final long DEFAULT_FORWARD_REPORT_INTERVAL = 7000;

	private static final Logger LOG = LoggerFactory.getLogger(QosReportForwarderThread.class);

	private final JobID jobId;
	private final StreamTaskQosCoordinator qosCoordinator;
	private final ActorRef jobManager;
	private long aggregationInterval;
	private boolean isRunning = false;
	private boolean isShuttingDown = false;

	private Set<Integer> runningInstances = new HashSet<Integer>();

	public QosReportForwarderThread(StreamTaskQosCoordinator qosCoordinator, Environment env) {
		this.jobId = env.getJobID();
		this.jobManager = env.getJobManager();
		this.qosCoordinator = qosCoordinator;
		this.aggregationInterval = qosCoordinator.getAggregationInterval();

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

	public long getAggregationInterval() {
		return this.aggregationInterval;
	}

	public int getSamplingProbability() {
		return this.qosCoordinator.getSamplingProbability();
	}

	public void addToNextReport(AbstractQosReportRecord record) {
	}

	/**
	 * (Re)start forwarding thread.
	 */
	private synchronized void ensureIsRunning() {
		if (!this.isRunning) {
			start();
			this.isRunning = true;
		}
	}

	/**
	 * Stop running thread. Its safe to restart the thread via {@link #ensureIsRunning()}.
	 */
	private synchronized void stopThread() {
		if (this.isRunning) {
			interrupt();
			this.isRunning = false;
		}
	}

	/**
	 * Stop running thread and cleanup.
	 * After calling this method, the forwarder can't be used anymore!
	 */
	public void shutdown() {
		LOG.info("Forwarder on job {} finished.", this.jobId);
		this.isShuttingDown = true;
		stopThread();
	}

	/**
	 * This ensures that the forwarder thread is running.
	 */
	public void registerTask(StreamTask vertex) {
		synchronized (this.runningInstances) {
			this.runningInstances.add(vertex.getInstanceID());
		}
		LOG.info("{} running operators after opening operator {}.", runningInstances.size(), vertex.getInstanceID());
		ensureIsRunning();
	}

	/**
	 * This stops the forwarder thread if no task is present anymore.
	 */
	public void unregisterTask(StreamTask vertex) {
		synchronized (this.runningInstances) {
			this.runningInstances.remove(vertex.getInstanceID());
			LOG.info("{} running tasks left after removing task {}.", runningInstances.size(), vertex.getInstanceID());

			if (this.runningInstances.isEmpty()) {
				stopThread();
			}
		}
	}
}
