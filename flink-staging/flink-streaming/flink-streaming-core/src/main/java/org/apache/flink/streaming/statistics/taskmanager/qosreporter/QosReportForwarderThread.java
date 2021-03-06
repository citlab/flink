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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.statistics.message.qosreport.AbstractQosReportRecord;
import org.apache.flink.streaming.statistics.message.qosreport.EdgeLatency;
import org.apache.flink.streaming.statistics.message.qosreport.EdgeStatistics;
import org.apache.flink.streaming.statistics.message.qosreport.QosReport;
import org.apache.flink.streaming.statistics.message.qosreport.VertexStatistics;
import org.apache.flink.streaming.statistics.util.QosStatisticsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class aggregates and forwards stream QoS report data (latencies,
 * throughput, etc) of the tasks of a single job running within the same
 * manager. Each task manager has one instance of this class per job. Qos report
 * data is pre-aggregated and shipped in a single message once every {@link #aggregationInterval}.
 * If no QoS data has been received, messages will be skipped. This class
 * starts its own thread as soon as there is at least on registered task and can
 * be shut down by invoking {@link #shutdown()}.
 *
 * This class is threadsafe.
 *
 * @author Bjoern Lohrmann, Sascha Wolke
 */
public class QosReportForwarderThread extends Thread {
	private static final Logger LOG = LoggerFactory.getLogger(QosReportForwarderThread.class);

	/** Keeps track of running instances of this class. */
	private final static HashMap<JobID, QosReportForwarderThread> runningForwarder = new HashMap<JobID, QosReportForwarderThread>();

	private final JobID jobID;

	private final long aggregationInterval;

	private final int samplingProbability;

	private volatile boolean isShuttingDown = false;

	private final HashMap<ExecutionAttemptID, StreamTask> registeredTasksInstances;

	/** Time until next report submission. */
	private long reportDueTime;

	private QosReport currentReport;

	private final LinkedBlockingQueue<AbstractQosReportRecord> pendingReportRecords;

	private final ArrayList<AbstractQosReportRecord> tmpRecords;

	public QosReportForwarderThread(JobID jobID, Configuration jobConf) {
		this.jobID = jobID;

		this.aggregationInterval = jobConf.getLong(
				QosStatisticsConfig.AGGREGATION_INTERVAL_KEY,
				QosStatisticsConfig.getAggregationIntervalMillis());
		this.samplingProbability = jobConf.getInteger(
				QosStatisticsConfig.SAMPLING_PROBABILITY_KEY,
				QosStatisticsConfig.getSamplingProbabilityPercent());

		this.registeredTasksInstances = new HashMap<ExecutionAttemptID, StreamTask>();

		this.currentReport = new QosReport();
		this.reportDueTime = System.currentTimeMillis();

		this.pendingReportRecords = new LinkedBlockingQueue<AbstractQosReportRecord>();
		this.tmpRecords = new ArrayList<AbstractQosReportRecord>();

		setName(String.format("QosReporterForwarderThread (JobID: %s)", jobID.toString()));
	}

	@Override
	public void run() {
		LOG.info("Forwarder on job {} started.", this.jobID);

		try {
			while (!interrupted() && !this.isShuttingDown) {

				this.processPendingReportRecords();
				this.sleepUntilReportDue();
				this.processPendingReportRecords();

				if (!this.currentReport.isEmpty()) {
					sendReport(currentReport);
				}

				shiftToNextReportingInterval();
			}
		} catch (InterruptedException e) {
		}

		LOG.info("Forwarder on job {} stopped.", this.jobID);
	}

	public long getAggregationInterval() {
		return this.aggregationInterval;
	}

	public int getSamplingProbability() {
		return this.samplingProbability;
	}

	/**
	 * Sends given report via first registered task environment to central statistics manager.
	 */
	private void sendReport(QosReport report) {
		Iterator<StreamTask> streamTasks = this.registeredTasksInstances.values().iterator();

		if (streamTasks.hasNext()) {
			report.setTimestamp();
			streamTasks.next().getEnvironment().reportCustomStatistic(report);
		} else {
			LOG.warn("No tasks registered. Can't send report. Dropping current report.");
		}
	}

	/**
	 * Start forwarding thread.
	 */
	private synchronized void ensureIsRunning() {
		if (!this.isAlive()) {
			start();
		}
	}

	/**
	 * Shutdown forwarder. After calling this method, the forwarder can't be used anymore!
	 */
	public void shutdown() {
		LOG.info("Forwarder on job {} finished.", this.jobID);
		this.isShuttingDown = true;
		interrupt();
	}

	public void addToNextReport(AbstractQosReportRecord record) {
		if (this.isShuttingDown) {
			return;
		}
		this.pendingReportRecords.add(record);
	}

	private void shiftToNextReportingInterval() {
		long now = System.currentTimeMillis();
		while (this.reportDueTime <= now) {
			this.reportDueTime = this.reportDueTime + this.aggregationInterval;
		}
		this.currentReport = new QosReport();
	}

	private void sleepUntilReportDue() throws InterruptedException {
		long sleepTime = Math.max(0, this.reportDueTime - System.currentTimeMillis());
		if (sleepTime > 0) {
			sleep(sleepTime);
		}
	}

	private void processPendingReportRecords() {
		this.pendingReportRecords.drainTo(this.tmpRecords);

		for (AbstractQosReportRecord record : this.tmpRecords) {
			if (record instanceof EdgeLatency) {
				this.currentReport.addEdgeLatency((EdgeLatency) record);
			} else if (record instanceof EdgeStatistics) {
				this.currentReport.addEdgeStatistics((EdgeStatistics) record);
			} else if (record instanceof VertexStatistics) {
				this.currentReport.addVertexStatistics((VertexStatistics) record);
			} else {
				LOG.error("Cannot process report record: {}.", record.getClass().getSimpleName());
			}
		}

		this.tmpRecords.clear();
	}

	/**
	 * This ensures that the forwarder thread is running.
	 */
	private void registerTask(StreamTask task) {
		if (this.isShuttingDown) {
			throw new RuntimeException("Can't register task after shutdown has called.");
		}

		this.registeredTasksInstances.put(task.getEnvironment().getExecutionId(), task);

		LOG.info("{} running operators after opening operator {}.",
				registeredTasksInstances.size(), task.getEnvironment().getExecutionId());

		ensureIsRunning();
	}

	/**
	 * This stops and clears the forwarder thread if no task is present anymore.
	 */
	public void unregisterTask(StreamTask vertex) {
		synchronized (runningForwarder) {
			this.registeredTasksInstances.remove(vertex.getEnvironment().getExecutionId());

			if (this.registeredTasksInstances.isEmpty()) {
				runningForwarder.remove(this.jobID);
				shutdown();
			}
		}

		LOG.info("{} running tasks left after removing task {}.",
				registeredTasksInstances.size(), vertex.getEnvironment().getExecutionId());
	}

	/**
	 * Creates a new job forwarder instance if no instance handles given task and register
	 * the task als active instance.
	 *
	 * After finishing the task, call {@link #unregisterTask(StreamTask)}.
	 */
	public static QosReportForwarderThread getOrCreateForwarderAndRegisterTask(StreamTask task) {

		QosReportForwarderThread forwarder;

		synchronized (runningForwarder) {
			Environment env = task.getEnvironment();
			JobID jobID = env.getJobID();
			forwarder = runningForwarder.get(jobID);

			if (forwarder == null) {
				Configuration jobConf = env.getJobConfiguration();
				forwarder = new QosReportForwarderThread(jobID, jobConf);

				runningForwarder.put(jobID, forwarder);
			}

			forwarder.registerTask(task);
		}

		return forwarder;
	}
}
