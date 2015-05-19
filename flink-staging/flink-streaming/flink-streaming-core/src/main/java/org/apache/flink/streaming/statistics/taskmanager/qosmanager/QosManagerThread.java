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

package org.apache.flink.streaming.statistics.taskmanager.qosmanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.JobID;
import org.apache.flink.streaming.statistics.JobGraphLatencyConstraint;
import org.apache.flink.streaming.statistics.jobmanager.autoscaling.ElasticTaskQosAutoScalingThread;
import org.apache.flink.streaming.statistics.message.AbstractQosMessage;
import org.apache.flink.streaming.statistics.message.QosManagerConstraintSummaries;
import org.apache.flink.streaming.statistics.message.qosreport.QosReport;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.buffers.OutputBufferLatencyManager;
import org.apache.flink.streaming.statistics.util.QosStatisticsConfig;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Implements a thread that serves as a Qos manager. It is started by invoking
 * {@link Thread#start()} and can by shut down with {@link #shutdown()}. It
 * continuously processes {@link AbstractQosMessage} objects from a
 * threadsafe queue and triggers Qos actions if necessary.
 * {@link #handOffStreamingData(AbstractQosMessage)} can be used to enqueue
 * data.
 * 
 * @author Bjoern Lohrmann, Sascha Wolke
 * 
 */
public class QosManagerThread extends Thread {

	private static final Log LOG = LogFactory.getLog(QosManagerThread.class);
	
	public final static long WAIT_BEFORE_FIRST_ADJUSTMENT = 10 * 1000;

	private final LinkedBlockingQueue<AbstractQosMessage> streamingDataQueue;

	private OutputBufferLatencyManager oblManager;

	private QosModel qosModel;

	private final ElasticTaskQosAutoScalingThread autoScalingThread;

	private final long adjustmentInterval;

	private long timeOfNextAdjustment;
	
	private class MessageStats {
		
		int noOfMessages = 0;
		int noOfEdgeLatencies = 0;
		int noOfVertexLatencies = 0;
		int noOfEdgeStatistics = 0;

		public void logAndReset(QosModel.State state) {
			LOG.debug(String.format("total messages: %d (edge: %d lats and %d stats | vertex: %d) || enqueued: %d || QosModel: %s",
							noOfMessages, noOfEdgeLatencies,
							noOfEdgeStatistics, noOfVertexLatencies,
							streamingDataQueue.size(),
							state.toString()));

			noOfMessages = 0;
			noOfEdgeLatencies = 0;
			noOfVertexLatencies = 0;
			noOfEdgeStatistics = 0;
		}

		public void updateWithReport(QosReport qosReport) {
			noOfEdgeLatencies += qosReport.getEdgeLatencies().size();
			noOfVertexLatencies += qosReport.getVertexStatistics().size();
			noOfEdgeStatistics += qosReport.getEdgeStatistics().size();
		}
	}

	public QosManagerThread(JobID jobID, QosModel qosModel,
			ElasticTaskQosAutoScalingThread autoScalingThread) {

		this.adjustmentInterval = QosStatisticsConfig.getAdjustmentIntervalMillis();

		this.timeOfNextAdjustment = QosUtils.alignToInterval(
				System.currentTimeMillis() + WAIT_BEFORE_FIRST_ADJUSTMENT,
				this.adjustmentInterval);
		
		this.qosModel = qosModel;
		this.autoScalingThread = autoScalingThread;
		this.streamingDataQueue = new LinkedBlockingQueue<AbstractQosMessage>();
		this.oblManager = new OutputBufferLatencyManager(jobID);
		this.setName(String.format("QosManagerThread (JobID: %s)", jobID.toString()));
	}

	@Override
	public void run() {
		LOG.info("Started Qos manager thread.");

		MessageStats stats = new MessageStats();

		try {
			while (!interrupted()) {
				AbstractQosMessage streamingData = this.streamingDataQueue
						.poll(50, TimeUnit.MILLISECONDS);

				if (streamingData != null) {
					stats.noOfMessages++;
					processStreamingData(stats, streamingData);
				}
				
				adjustIfNecessary(stats);
			}

		} catch (InterruptedException e) {
			// do nothing
		} catch (Exception e) {
			LOG.error("Error in QosManager thread", e);
		} finally {
			this.cleanUp();
		}

		LOG.info("Stopped Qos Manager thread");
	}

	private void adjustIfNecessary(MessageStats stats)
			throws InterruptedException {

		long beginAdjustTime = System.currentTimeMillis();
		if (!isAdjustmentNecessary(beginAdjustTime)) {
			return;
		}

		List<QosConstraintSummary> constraintSummaries = null;
		
		if (this.qosModel.isReady()) {
			QosConstraintViolationListener listener = this.oblManager
					.getQosConstraintViolationListener();

			constraintSummaries = this.qosModel.findQosConstraintViolationsAndSummarize(listener);

			this.oblManager.applyAndSendBufferAdjustments(beginAdjustTime);

		}
		
		if(constraintSummaries == null) {
			constraintSummaries = createEmptyConstraintSummaries();
		}
		
		sendConstraintSummariesToJm(constraintSummaries, beginAdjustTime);

		long now = System.currentTimeMillis();
		stats.logAndReset(qosModel.getState());
		this.refreshTimeOfNextAdjustment(now);
	}
	
	private List<QosConstraintSummary> createEmptyConstraintSummaries() {
		List<QosConstraintSummary> emptySummaries = new LinkedList<QosConstraintSummary>();
		
		for(JobGraphLatencyConstraint constraint : qosModel.getJobGraphLatencyConstraints()) {		
			emptySummaries.add(new QosConstraintSummary(constraint, new QosConstraintViolationReport(constraint)));
		}
		
		return emptySummaries;
	}

	private boolean isAdjustmentNecessary(long now) {
		return now >= this.timeOfNextAdjustment;
	}

	private void processStreamingData(MessageStats stats,
			AbstractQosMessage streamingData) {
		
		if (streamingData instanceof QosReport) {
			QosReport qosReport = (QosReport) streamingData;
			this.qosModel.processQosReport(qosReport);
			stats.updateWithReport(qosReport);
//		} else if (streamingData instanceof ChainUpdates) {
//			this.qosModel.processChainUpdates((ChainUpdates) streamingData);
		}
	}
	
	private void refreshTimeOfNextAdjustment(long now) {
		while (this.timeOfNextAdjustment <= now) {
			this.timeOfNextAdjustment += this.adjustmentInterval;
		}
	}

	private void sendConstraintSummariesToJm(
			List<QosConstraintSummary> constraintSummaries, long timestamp)
			throws InterruptedException {

		this.autoScalingThread.enqueueMessage(
			new QosManagerConstraintSummaries(timestamp, constraintSummaries)
		);
	}

	private void cleanUp() {
		this.streamingDataQueue.clear();
		this.qosModel = null;
		this.oblManager = null;
	}

	public void shutdown() {
		this.interrupt();
	}

	public void handOffStreamingData(AbstractQosMessage data) {
		this.streamingDataQueue.add(data);
	}
}
