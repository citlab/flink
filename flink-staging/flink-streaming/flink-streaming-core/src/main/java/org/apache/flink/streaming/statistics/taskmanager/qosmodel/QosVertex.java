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

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.streaming.statistics.message.action.QosReporterConfig;
import org.apache.flink.streaming.statistics.message.action.VertexQosReporterConfig;
import org.apache.flink.streaming.statistics.message.qosreport.AbstractQosReportRecord;
import org.apache.flink.streaming.statistics.message.qosreport.VertexStatistics;

import java.util.ArrayList;

/**
 * This class models a Qos vertex as part of a Qos graph. It is equivalent to an
 * {@link org.apache.flink.runtime.executiongraph.ExecutionVertex}.
 *
 * Instances of this class contain sparse lists of input and output gates.
 * Sparseness means that, for example if the vertex has an output gate with
 * index 1 it may not have an output gate with index 0. This stems from the
 * fact, that the Qos graph itself only contains those group vertices and group
 * edges from the execution graph, that are covered by a constraint.
 *
 * Statistics from one task manager with different {@link QosReporterID.Vertex} are collected
 * in its {@ling qosData}.
 *
 * This data class residents on job manager side and is never transferred.
 *
 * @author Bjoern Lohrmann, Sascha Wolke
 *
 */
public class QosVertex implements QosGraphMember {

	private ExecutionVertex executionVertex;

	private QosGroupVertex groupVertex;

	private ArrayList<QosGate> inputGates;

	private ArrayList<QosGate> outputGates;

	private VertexQosData qosData;

	public QosVertex(ExecutionVertex executionVertex) {
		this.executionVertex = executionVertex;
		this.inputGates = new ArrayList<QosGate>();
		this.outputGates = new ArrayList<QosGate>();
	}

	public ExecutionAttemptID getID() {
		return this.executionVertex.getCurrentExecutionAttempt().getAttemptId();
	}

	public QosGate getInputGate(int gateIndex) {
		if (gateIndex < this.inputGates.size()) {
			return this.inputGates.get(gateIndex);
		} else {
			return null;
		}
	}

	public void setInputGate(QosGate inputGate) {
		if (!inputGate.isInputGate()) {
			throw new RuntimeException("Input gate expected.");
		}

		if (inputGate.getGateIndex() >= this.inputGates.size()) {
			this.fillWithNulls(this.inputGates, inputGate.getGateIndex() + 1);
		}

		inputGate.setVertex(this);
		this.inputGates.set(inputGate.getGateIndex(), inputGate);
	}

	public QosGate getOutputGate(int gateIndex) {
		if (gateIndex < this.outputGates.size()) {
			return this.outputGates.get(gateIndex);
		} else {
			return null;
		}
	}

	public void setOutputGate(QosGate outputGate) {
		if (!outputGate.isOutputGate()) {
			throw new RuntimeException("Output gate expected.");
		}

		if (outputGate.getGateIndex() >= this.outputGates.size()) {
			this.fillWithNulls(this.outputGates, outputGate.getGateIndex() + 1);
		}

		outputGate.setVertex(this);
		this.outputGates.set(outputGate.getGateIndex(), outputGate);
	}

	private <T> void fillWithNulls(ArrayList<T> list, int targetSize) {
		int toAdd = targetSize - list.size();

		for (int i = 0; i < toAdd; i++) {
			list.add(null);
		}
	}

	public Instance getExecutingInstance() {
		return this.executionVertex.getCurrentAssignedResource().getInstance();
	}

	@Override
	public VertexQosData getQosData() {
		return this.qosData;
	}

	public void setQosData(VertexQosData qosData) {
		this.qosData = qosData;
	}

	@Override
	public void processStatistics(QosReporterConfig reporterConfig,
			AbstractQosReportRecord statistic, long now) {

		if (reporterConfig instanceof VertexQosReporterConfig && statistic instanceof VertexStatistics) {
			VertexQosReporterConfig vertexReporterConfig = (VertexQosReporterConfig) reporterConfig;
			qosData.addVertexStatisticsMeasurement(
					vertexReporterConfig.getInputGateIndex(),
					vertexReporterConfig.getOutputGateIndex(),
					now,
					(VertexStatistics) statistic);
		} else {
			throw new RuntimeException("Unknown statistic type received: "
					+ statistic.getClass().getSimpleName());
		}
	}

	public String getName() {
		return this.executionVertex.getTaskNameWithSubtaskIndex();
	}

	@Override
	public String toString() {
		return getName();
	}

	public int getMemberIndex() {
		return this.executionVertex.getParallelSubtaskIndex();
	}

	public void setGroupVertex(QosGroupVertex qosGroupVertex) {
		this.groupVertex = qosGroupVertex;
	}

	public QosGroupVertex getGroupVertex() {
		return this.groupVertex;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		if (obj.getClass() != this.getClass()) {
			return false;
		}
		QosVertex rhs = (QosVertex) obj;
		return this.groupVertex.getJobVertexID().equals(rhs.groupVertex.getJobVertexID())
				&& this.getMemberIndex() == rhs.getMemberIndex();
	}

	@Override
	public boolean isVertex() {
		return true;
	}

	@Override
	public boolean isEdge() {
		return false;
	}
}
