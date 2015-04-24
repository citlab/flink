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

import java.util.ArrayList;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosGate.GateType;

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
 * @author Bjoern Lohrmann
 *
 */
public class QosVertex implements QosGraphMember {

	private ExecutionAttemptID executionAttemptID;

	private QosGroupVertex groupVertex;

	private InstanceConnectionInfo executingInstance;

	private ArrayList<QosGate> inputGates;

	private ArrayList<QosGate> outputGates;

	private int memberIndex;

	private String name;

	/**
	 * Only for use on the task manager side. Will not be transferred.
	 */
	private transient VertexQosData qosData;

	public QosVertex(ExecutionAttemptID executionID, String name, InstanceConnectionInfo executingInstance, int memberIndex) {
		this.executionAttemptID = executionID;
		this.name = name;
		this.executingInstance = executingInstance;
		this.inputGates = new ArrayList<QosGate>();
		this.outputGates = new ArrayList<QosGate>();
		this.memberIndex = memberIndex;
	}

	public QosVertex(String name, int memberIndex) {
		this.name = name;
		this.memberIndex = memberIndex;
	}

	public ExecutionAttemptID getID() {
		return this.executionAttemptID;
	}

	public QosGate getInputGate(int gateIndex) {
		try {
			return this.inputGates.get(gateIndex);
		} catch (IndexOutOfBoundsException e) {
			return null;
		}
	}

	public void setInputGate(QosGate inputGate) {
		if (inputGate.getGateIndex() >= this.inputGates.size()) {
			this.fillWithNulls(this.inputGates, inputGate.getGateIndex() + 1);
		}

		inputGate.setVertex(this);
		inputGate.setGateType(GateType.INPUT_GATE);
		this.inputGates.set(inputGate.getGateIndex(), inputGate);
	}

	public QosGate getOutputGate(int gateIndex) {
		try {
			return this.outputGates.get(gateIndex);
		} catch (IndexOutOfBoundsException e) {
			return null;
		}
	}

	public void setOutputGate(QosGate outputGate) {
		if (outputGate.getGateIndex() >= this.outputGates.size()) {
			this.fillWithNulls(this.outputGates, outputGate.getGateIndex() + 1);
		}

		outputGate.setVertex(this);
		outputGate.setGateType(GateType.OUTPUT_GATE);
		this.outputGates.set(outputGate.getGateIndex(), outputGate);
	}

	private <T> void fillWithNulls(ArrayList<T> list, int targetSize) {
		int toAdd = targetSize - list.size();

		for (int i = 0; i < toAdd; i++) {
			list.add(null);
		}
	}

	public InstanceConnectionInfo getExecutingInstance() {
		return this.executingInstance;
	}

	public VertexQosData getQosData() {
		return this.qosData;
	}

	public void setQosData(VertexQosData qosData) {
		this.qosData = qosData;
	}

	public String getName() {
		return this.name;
	}

	@Override
	public String toString() {
		return this.name;
	}

	public QosVertex emptyClone() {
		return new QosVertex(this.name, this.memberIndex);
	}

	public void setMemberIndex(int memberIndex) {
		this.memberIndex = memberIndex;
	}

	public int getMemberIndex() {
		return this.memberIndex;
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
				&& this.memberIndex == rhs.memberIndex;
	}

	public static QosVertex fromExecutionVertex(ExecutionVertex executionVertex) {
		return new QosVertex(
				executionVertex.getCurrentExecutionAttempt().getAttemptId(),
				executionVertex.getSimpleName(),
				executionVertex.getCurrentAssignedResourceLocation(),
				executionVertex.getParallelSubtaskIndex());
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
