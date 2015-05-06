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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosEdge;

import java.io.IOException;
import java.util.List;

public class QosGroupEdgeSummary implements QosGroupElementSummary {

	/**
	 * The number of active member group edges.
	 */
	private int activeEdges;

	/**
	 * The mean output buffer latency of the group edge's {@link QosEdge}
	 * members.
	 */
	private double outputBufferLatencyMean;

	/**
	 * The average remaining transport latency (i.e. channel latency without
	 * output buffer latency) of the group edge's {@link QosEdge} members.
	 */
	private double transportLatencyMean;

	/**
	 * The number of source member vertices actively writing into the group
	 * edge's {@link QosEdge} member edges.
	 */
	private int activeEmitterVertices;

	/**
	 * The number of target member vertices actively reading from the group
	 * edge's {@link QosEdge} member edges. This is actually an int but
	 * represented as a double.
	 */
	private int activeConsumerVertices;

	/**
	 * The mean number of records per second that the source member vertices
	 * write into the group edge's {@link QosEdge} member edges. Example: With
	 * one source member vertex that emits 20 rec/s and one that emits 10 rec/s,
	 * this variable will be 15.
	 */
	private double meanEmissionRate;

	/**
	 * The mean number of records per second that the target member vertices
	 * read from the group edge's {@link QosEdge} member edges. Example: With
	 * one target member vertex consuming 20 rec/s and one consuming 10 rec/s,
	 * this variable will be 15.
	 */
	private double meanConsumptionRate;

	/**
	 * Each active target member vertex reads records from the group edge's
	 * {@link QosEdge} member edges. For such a member vertex we measure the
	 * mean time it takes the vertex to process a record. This is also referred
	 * to as "vertex latency". Since vertex latency is not constant we compute
	 * its mean and variance for each member. This variable then holds the mean
	 * of the mean vertex latencies of all active target member vertices.
	 * 
	 * <p>
	 * In general the following equation holds: meanConsumerVertexLatency <=
	 * 1/meanConsumptionRate, because consumption is throttled when no input is
	 * available. If input is always available, then meanConsumerVertexLatency
	 * =~ 1/meanConsumptionRate.
	 * </p>
	 */
	private double meanConsumerVertexLatency;

	/**
	 * See {@link #meanConsumerVertexLatency}. This is the mean coefficient of variation of
	 * vertex latencies.
	 */
	private double meanConsumerVertexLatencyCV;

	private double meanConsumerVertexInterarrivalTime;

	private double meanConsumerVertexInterarrivalTimeCV;

	public QosGroupEdgeSummary() {
	}

	public int getActiveEdges() {
		return activeEdges;
	}

	public void setActiveEdges(int activeEdges) {
		this.activeEdges = activeEdges;
	}

	public double getOutputBufferLatencyMean() {
		return outputBufferLatencyMean;
	}

	public void setOutputBufferLatencyMean(double outputBufferLatencyMean) {
		this.outputBufferLatencyMean = outputBufferLatencyMean;
	}

	public double getTransportLatencyMean() {
		return transportLatencyMean;
	}

	public void setTransportLatencyMean(double transportLatencyMean) {
		this.transportLatencyMean = transportLatencyMean;
	}

	public int getActiveEmitterVertices() {
		return activeEmitterVertices;
	}

	public void setActiveEmitterVertices(int activeEmitterVertices) {
		this.activeEmitterVertices = activeEmitterVertices;
	}

	public int getActiveConsumerVertices() {
		return activeConsumerVertices;
	}

	public void setActiveConsumerVertices(int activeConsumerVertices) {
		this.activeConsumerVertices = activeConsumerVertices;
	}

	public double getMeanEmissionRate() {
		return meanEmissionRate;
	}

	public void setMeanEmissionRate(double meanEmissionRate) {
		this.meanEmissionRate = meanEmissionRate;
	}

	public double getMeanConsumptionRate() {
		return meanConsumptionRate;
	}

	public void setMeanConsumptionRate(double meanConsumptionRate) {
		this.meanConsumptionRate = meanConsumptionRate;
	}

	public double getMeanConsumerVertexLatency() {
		return meanConsumerVertexLatency;
	}

	public void setMeanConsumerVertexLatency(double meanConsumerVertexLatency) {
		this.meanConsumerVertexLatency = meanConsumerVertexLatency;
	}

	public double getMeanConsumerVertexLatencyCV() {
		return meanConsumerVertexLatencyCV;
	}

	public void setMeanConsumerVertexLatencyCV(
			double meanConsumerVertexLatencyCV) {
		this.meanConsumerVertexLatencyCV = meanConsumerVertexLatencyCV;
	}

	public double getMeanConsumerVertexInterarrivalTime() {
		return meanConsumerVertexInterarrivalTime;
	}

	public void setMeanConsumerVertexInterarrivalTime(
			double meanConsumerVertexInterarrivalTime) {
		this.meanConsumerVertexInterarrivalTime = meanConsumerVertexInterarrivalTime;
	}

	public double getMeanConsumerVertexInterarrivalTimeCV() {
		return meanConsumerVertexInterarrivalTimeCV;
	}

	public void setMeanConsumerVertexInterarrivalTimeCV(
			double meanConsumerVertexInterarrivalTimeCV) {
		this.meanConsumerVertexInterarrivalTimeCV = meanConsumerVertexInterarrivalTimeCV;
	}

	@Override
	public boolean isVertex() {
		return false;
	}

	@Override
	public boolean isEdge() {
		return true;
	}

	@Override
	public boolean hasData() {
		return (activeEdges == -1 || activeEdges > 0) && activeConsumerVertices > 0
				&& activeEmitterVertices > 0;
	}

	@Override
	public void merge(List<QosGroupElementSummary> elemSummaries) {
		for (QosGroupElementSummary elemSum : elemSummaries) {
			QosGroupEdgeSummary toMerge = (QosGroupEdgeSummary) elemSum;

			activeEdges += toMerge.activeEdges;

			outputBufferLatencyMean += toMerge.activeEdges
					* toMerge.outputBufferLatencyMean;

			transportLatencyMean += toMerge.activeEdges
					* toMerge.transportLatencyMean;

			activeEmitterVertices += toMerge.activeEmitterVertices;

			meanEmissionRate += toMerge.activeEmitterVertices
					* toMerge.meanEmissionRate;

			activeConsumerVertices += toMerge.activeConsumerVertices;

			meanConsumptionRate += toMerge.activeConsumerVertices
					* toMerge.meanConsumptionRate;

			meanConsumerVertexLatency += toMerge.activeConsumerVertices
					* toMerge.meanConsumerVertexLatency;

			meanConsumerVertexLatencyCV += toMerge.activeConsumerVertices
					* toMerge.meanConsumerVertexLatencyCV;

			meanConsumerVertexInterarrivalTime += toMerge.activeConsumerVertices
					* toMerge.meanConsumerVertexInterarrivalTime;

			meanConsumerVertexInterarrivalTimeCV += toMerge.activeConsumerVertices
					* toMerge.meanConsumerVertexInterarrivalTimeCV;
		}

		if (hasData()) {
			outputBufferLatencyMean /= activeEdges;
			transportLatencyMean /= activeEdges;

			meanEmissionRate /= activeEmitterVertices;

			meanConsumptionRate /= activeConsumerVertices;
			meanConsumerVertexLatency /= activeConsumerVertices;
			meanConsumerVertexLatencyCV /= activeConsumerVertices;
			meanConsumerVertexInterarrivalTime /= activeConsumerVertices;
			meanConsumerVertexInterarrivalTimeCV /= activeConsumerVertices;
		}
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(activeEdges);
		out.writeDouble(outputBufferLatencyMean);
		out.writeDouble(transportLatencyMean);
		
		out.writeInt(activeEmitterVertices);
		out.writeDouble(meanEmissionRate);

		out.writeInt(activeConsumerVertices);
		out.writeDouble(meanConsumptionRate);
		out.writeDouble(meanConsumerVertexLatency);
		out.writeDouble(meanConsumerVertexLatencyCV);
		out.writeDouble(meanConsumerVertexInterarrivalTime);
		out.writeDouble(meanConsumerVertexInterarrivalTimeCV);

	}

	@Override
	public void read(DataInputView in) throws IOException {
		activeEdges = in.readInt();
		outputBufferLatencyMean = in.readDouble();
		transportLatencyMean = in.readDouble();
		
		activeEmitterVertices = in.readInt();
		meanEmissionRate = in.readDouble();

		activeConsumerVertices = in.readInt();
		meanConsumptionRate = in.readDouble();
		meanConsumerVertexLatency = in.readDouble();
		meanConsumerVertexLatencyCV = in.readDouble();
		meanConsumerVertexInterarrivalTime = in.readDouble();
		meanConsumerVertexInterarrivalTimeCV = in.readDouble();
	}
}
