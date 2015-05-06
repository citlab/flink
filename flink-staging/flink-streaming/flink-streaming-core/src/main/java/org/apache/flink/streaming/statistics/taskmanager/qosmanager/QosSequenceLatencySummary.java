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

import org.apache.flink.streaming.statistics.JobGraphSequence;
import org.apache.flink.streaming.statistics.SequenceElement;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.buffers.ValueHistory;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.EdgeQosData;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosEdge;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosGraphMember;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosVertex;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.VertexQosData;

import java.util.List;

public class QosSequenceLatencySummary {
	
	private final int[][] inputOutputGateCombinations;
	private final double[][] memberLatencies;
	private int noOfEdges;
	private int noOfVertices;
	private double sequenceLatency;
	private double vertexLatencySum;
	private double transportLatencySum;
	private boolean isMemberQosDataFresh;

	public QosSequenceLatencySummary(JobGraphSequence jobGraphSequence) {
		this.inputOutputGateCombinations = new int[jobGraphSequence.size()][];
		this.noOfEdges = 0;
		this.noOfVertices = 0;
		
		this.memberLatencies = new double[jobGraphSequence.size()][];
		for (SequenceElement sequenceElement : jobGraphSequence) {
			int index = sequenceElement.getIndexInSequence();

			this.inputOutputGateCombinations[index] = new int[2];
			if (sequenceElement.isVertex()) {
				this.noOfVertices++;
				this.memberLatencies[index] = new double[1];
				this.inputOutputGateCombinations[index][0] = sequenceElement
						.getInputGateIndex();
				this.inputOutputGateCombinations[index][1] = sequenceElement
						.getOutputGateIndex();
			} else {
				this.noOfEdges++;
				this.memberLatencies[index] = new double[2];
				this.inputOutputGateCombinations[index][0] = sequenceElement
						.getOutputGateIndex();
				this.inputOutputGateCombinations[index][1] = sequenceElement
						.getInputGateIndex();
			}
		}
	}
	
	public void update(List<QosGraphMember> sequenceMembers) {
		this.sequenceLatency = 0;
		this.vertexLatencySum = 0;
		this.transportLatencySum = 0;
		this.isMemberQosDataFresh = true;
		
		int index = 0;
		for (QosGraphMember member : sequenceMembers) {
			if (member.isVertex()) {
				VertexQosData vertexQos = ((QosVertex) member).getQosData();

				int inputGateIndex = this.inputOutputGateCombinations[index][0];

				this.memberLatencies[index][0] = vertexQos.getLatencyInMillis(inputGateIndex);
				sequenceLatency += this.memberLatencies[index][0];
				vertexLatencySum += this.memberLatencies[index][0];
			} else {
				EdgeQosData edgeQos = ((QosEdge) member).getQosData();
				this.memberLatencies[index][0] = edgeQos.estimateOutputBufferLatencyInMillis();
				this.memberLatencies[index][1] = edgeQos.estimateTransportLatencyInMillis();
				sequenceLatency += edgeQos.getChannelLatencyInMillis();
				transportLatencySum += this.memberLatencies[index][1];
				this.isMemberQosDataFresh = this.isMemberQosDataFresh && hasFreshValues((QosEdge) member);
			}

			index++;
		}
	}
	
	private boolean hasFreshValues(QosEdge edge) {
		EdgeQosData edgeQos = edge.getQosData();

		ValueHistory<Integer> targetOblHistory = edgeQos.getTargetObltHistory();
		return !targetOblHistory.hasEntries()
				|| edgeQos.hasNewerData(targetOblHistory.getLastEntry().getTimestamp());
	}

	public double[][] getMemberLatencies() {
		return memberLatencies;
	}

	public double getSequenceLatency() {
		return sequenceLatency;
	}

	public double getVertexLatencySum() {
		return vertexLatencySum;
	}
	
	public double getTransportLatencySum() {
		return transportLatencySum;
	}
	
	public boolean isMemberQosDataFresh() {
		return this.isMemberQosDataFresh;
	}

	public int getNoOfEdges() {
		return noOfEdges;
	}

	public int getNoOfVertices() {
		return noOfVertices;
	}	
}
