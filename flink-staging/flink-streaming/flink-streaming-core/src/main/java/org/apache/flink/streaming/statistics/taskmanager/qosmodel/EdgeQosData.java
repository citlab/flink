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

package org.apache.flink.streaming.statistics.taskmanager.qosmodel;

import org.apache.flink.streaming.statistics.message.qosreport.EdgeStatistics;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.buffers.ValueHistory;
import org.apache.flink.streaming.statistics.util.QosStatisticsConfig;

/**
 * Instances of this class hold Qos data (latency, throughput, ...) of a
 * {@link QosEdge}.
 *
 * @author Bjoern Lohrmann
 *
 */
public class EdgeQosData implements QosData {

	private QosEdge edge;

	private QosStatistic latencyInMillisStatistic;

	private QosStatistic throughputInMbitStatistic;

	private QosStatistic outputBufferLifetimeStatistic;

	private QosStatistic recordsPerBufferStatistic;

	private QosStatistic recordsPerSecondStatistic;

	private boolean isInChain;

	private ValueHistory<Integer> targetObltHistory;


	public EdgeQosData(QosEdge edge) {
		this.edge = edge;
		this.isInChain = false;
		this.latencyInMillisStatistic = new QosStatistic(QosStatisticsConfig.computeQosStatisticWindowSize());
		this.throughputInMbitStatistic = new QosStatistic(QosStatisticsConfig.computeQosStatisticWindowSize());
		this.outputBufferLifetimeStatistic = new QosStatistic(
				QosStatisticsConfig.computeQosStatisticWindowSize());
		this.recordsPerBufferStatistic = new QosStatistic(QosStatisticsConfig.computeQosStatisticWindowSize());
		this.recordsPerSecondStatistic = new QosStatistic(QosStatisticsConfig.computeQosStatisticWindowSize());
		this.targetObltHistory = new ValueHistory<Integer>(2);
	}

	public QosEdge getEdge() {
		return this.edge;
	}

	public double estimateOutputBufferLatencyInMillis() {
		double channelLatency = getChannelLatencyInMillis();
		double oblt = getOutputBufferLifetimeInMillis();

		if (channelLatency == -1 || oblt == -1) {
			return -1;
		}

		double recordsPerBuffer = getRecordsPerBuffer();
		if (Math.abs(recordsPerBuffer - 1) < 0.001) {
			// pathological corner case: record emissions are very infrequent
			return oblt;
		} else {
			return Math.min(channelLatency, oblt / 2);
		}
	}

	public int proposeOutputBufferLifetimeForOutputBufferLatencyTarget(int targetObl) {
		double recordsPerBuffer = getRecordsPerBuffer();
		if (Math.abs(recordsPerBuffer - 1) < 0.001) {
			return targetObl;
		} else {
			return targetObl * 2;
		}
	}

	public double estimateTransportLatencyInMillis() {
		double channelLatency = getChannelLatencyInMillis();
		double obl = estimateOutputBufferLatencyInMillis();

		if (channelLatency == -1 || obl == -1) {
			return -1;
		}

		return Math.max(0, channelLatency - obl);
	}

	public double getChannelLatencyInMillis() {
		if (this.latencyInMillisStatistic.hasValues()) {
			return this.latencyInMillisStatistic.getMean();
		}

		return -1;
	}

	public double getChannelThroughputInMbit() {
		if (this.throughputInMbitStatistic.hasValues()) {
			return this.throughputInMbitStatistic.getMean();
		}
		return -1;
	}

	public double getOutputBufferLifetimeInMillis() {
		if (isInChain) {
			return 0;
		}
		if (this.outputBufferLifetimeStatistic.hasValues()) {
			return this.outputBufferLifetimeStatistic.getMean();
		}
		return -1;
	}

	public double getRecordsPerBuffer() {
		if (this.recordsPerBufferStatistic.hasValues()) {
			return this.recordsPerBufferStatistic.getMean();
		}
		return -1;
	}

	public double getRecordsPerSecond() {
		if (this.recordsPerSecondStatistic.hasValues()) {
			return this.recordsPerSecondStatistic.getMean();
		}
		return -1;
	}

	public void addLatencyMeasurement(long timestamp, double latencyInMillis) {
		QosValue value = new QosValue(latencyInMillis, timestamp);
		this.latencyInMillisStatistic.addValue(value);
	}

	public void addOutputChannelStatisticsMeasurement(long timestamp,
			EdgeStatistics stats) {

		QosValue throughput = new QosValue(stats.getThroughput(), timestamp);
		this.throughputInMbitStatistic.addValue(throughput);

		QosValue outputBufferLifetime = new QosValue(
				stats.getOutputBufferLifetime(), timestamp);
		this.outputBufferLifetimeStatistic.addValue(outputBufferLifetime);

		QosValue recordsPerBuffer = new QosValue(stats.getRecordsPerBuffer(),
				timestamp);
		this.recordsPerBufferStatistic.addValue(recordsPerBuffer);

		QosValue recordsPerSecond = new QosValue(stats.getRecordsPerSecond(),
				timestamp);
		this.recordsPerSecondStatistic.addValue(recordsPerSecond);
	}

	public void setIsInChain(boolean isInChain) {
		this.isInChain = isInChain;
		outputBufferLifetimeStatistic.clear();
		recordsPerBufferStatistic.clear();
		recordsPerSecondStatistic.clear();
	}

	public boolean isInChain() {
		return this.isInChain;
	}

	private boolean isChannelLatencyNewerThan(long thresholdTimestamp) {
		return latencyInMillisStatistic.hasValues()
				&& latencyInMillisStatistic.getOldestValue().getTimestamp() >= thresholdTimestamp;
	}

	private boolean isOutputBufferLifetimeNewerThan(long thresholdTimestamp) {
		if (isInChain) {
			return true;
		}
		return outputBufferLifetimeStatistic.hasValues()
				&& outputBufferLifetimeStatistic.getOldestValue().getTimestamp() >= thresholdTimestamp;
	}

	public boolean hasNewerData(long thresholdTimestamp) {
		return isChannelLatencyNewerThan(thresholdTimestamp)
			&& isOutputBufferLifetimeNewerThan(thresholdTimestamp);
	}

	public void dropOlderData(long thresholdTimestamp) {
		if (!isChannelLatencyNewerThan(thresholdTimestamp)) {
			latencyInMillisStatistic.clear();
		}

		if (!isInChain()
				&& !isOutputBufferLifetimeNewerThan(thresholdTimestamp)) {
			throughputInMbitStatistic.clear();
			outputBufferLifetimeStatistic.clear();
			recordsPerBufferStatistic.clear();
			recordsPerSecondStatistic.clear();
		}
	}

	public ValueHistory<Integer> getTargetObltHistory() {
		return this.targetObltHistory;
	}
}
