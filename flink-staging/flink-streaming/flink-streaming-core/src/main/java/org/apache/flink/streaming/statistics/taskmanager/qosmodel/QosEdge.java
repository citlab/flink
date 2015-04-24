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

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

/**
 * This class models a Qos edge as part of a Qos graph. It is equivalent to an
 * {@link org.apache.flink.runtime.executiongraph.ExecutionEdge}.
 *
 * @author Bjoern Lohrmann
 *
 */
public class QosEdge implements QosGraphMember {

	private final IntermediateResultPartitionID intermediateResultPartitionID;

	private final int consumedSubpartitionIndex;

	private QosGate outputGate;

	private QosGate inputGate;

	/**
	 * The index of this edge in the output gate's list of edges.
	 */
	private int outputGateEdgeIndex;

	/**
	 * The index of this edge in the input gate's list of edges.
	 */
	private int inputGateEdgeIndex;

	/**
	 * Only for use on the task manager side. Will not be transferred.
	 */
	private transient EdgeQosData qosData;

	public QosEdge(IntermediateResultPartitionID intermediateResultPartitionID,
			int consumedSubpartitionIndex,
			int outputGateEdgeIndex, int inputGateEdgeIndex) {

		this.intermediateResultPartitionID = intermediateResultPartitionID;
		this.consumedSubpartitionIndex = consumedSubpartitionIndex;
		this.outputGateEdgeIndex = outputGateEdgeIndex;
		this.inputGateEdgeIndex = inputGateEdgeIndex;
	}

	/**
	 * Returns the outputGate.
	 *
	 * @return the outputGate
	 */
	public QosGate getOutputGate() {
		return this.outputGate;
	}

	/**
	 * Sets the outputGate to the specified value.
	 *
	 * @param outputGate
	 *            the outputGate to set
	 */
	public void setOutputGate(QosGate outputGate) {
		this.outputGate = outputGate;
		this.outputGate.addEdge(this);
	}

	/**
	 * Returns the inputGate.
	 *
	 * @return the inputGate
	 */
	public QosGate getInputGate() {
		return this.inputGate;
	}

	/**
	 * Sets the inputGate to the specified value.
	 *
	 * @param inputGate
	 *            the inputGate to set
	 */
	public void setInputGate(QosGate inputGate) {
		this.inputGate = inputGate;
		this.inputGate.addEdge(this);
	}

	/**
	 * Returns the outputGateEdgeIndex.
	 *
	 * @return the outputGateEdgeIndex
	 */
	public int getOutputGateEdgeIndex() {
		return this.outputGateEdgeIndex;
	}

	/**
	 * Returns the inputGateEdgeIndex.
	 *
	 * @return the inputGateEdgeIndex
	 */
	public int getInputGateEdgeIndex() {
		return this.inputGateEdgeIndex;
	}

	public IntermediateResultPartitionID getIntermediateResultPartitionID() {
		return intermediateResultPartitionID;
	}

	public int getConsumedSubpartitionIndex() {
		return consumedSubpartitionIndex;
	}

	public EdgeQosData getQosData() {
		return this.qosData;
	}

	public void setQosData(EdgeQosData qosData) {
		this.qosData = qosData;
	}

	@Override
	public String toString() {
		return String.format("%s->%s", this.getOutputGate().getVertex()
				.getName(), this.getInputGate().getVertex().getName());
	}

	public QosEdge cloneWithoutGates() {
		QosEdge clone = new QosEdge(this.intermediateResultPartitionID,
				this.consumedSubpartitionIndex,
				this.outputGateEdgeIndex, this.inputGateEdgeIndex);
		return clone;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}

		QosEdge other = (QosEdge) obj;
		return this.intermediateResultPartitionID.equals(other.intermediateResultPartitionID)
				&& this.consumedSubpartitionIndex == other.consumedSubpartitionIndex;
	}

	@Override
	public boolean isVertex() {
		return false;
	}

	@Override
	public boolean isEdge() {
		return true;
	}
}
