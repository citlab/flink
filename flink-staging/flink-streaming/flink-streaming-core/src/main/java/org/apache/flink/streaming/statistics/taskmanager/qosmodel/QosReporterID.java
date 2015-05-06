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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.statistics.message.action.VertexQosReporterConfig;

import java.io.Serializable;

/**
 * Identifies a Qos Reporter. The ID is deterministically constructed from the
 * Qos graph member element (vertex/edge) that it reports on.
 *
 * @author Bjoern Lohrmann, Sascha Wolke
 */
public abstract class QosReporterID implements Serializable {

	public static class Vertex extends QosReporterID {

		private final int subTaskIndex;
		private final IntermediateDataSetID inputDataSetID;
		private final IntermediateDataSetID outputDataSetID;
		private final int precomputedHash;

		/**
		 * Creates a new Qos reporter ID. Initializes Vertex.
		 *
		 * @param subTaskIndex
		 *            The sub task index of the vertex that is reported on.
		 * @param inputDataSetID
		 *            The ID of vertex's input data set (gate) that is reported on.
		 *            May be null for dummy reporters.
		 * @param outputDataSetID
		 *            The ID of vertex's output data set (gate) that is reported on.
		 *            May be null for dummy reporters.
		 */
		public Vertex(int subTaskIndex,
						IntermediateDataSetID inputDataSetID,
						IntermediateDataSetID outputDataSetID) {

			this.subTaskIndex = subTaskIndex;
			this.inputDataSetID = inputDataSetID;
			this.outputDataSetID = outputDataSetID;
			this.precomputedHash = precomputeHash();
		}

		public int getSubTaskIndex() {
			return subTaskIndex;
		}

		public IntermediateDataSetID getInputDataSetID() {
			return inputDataSetID;
		}

		public IntermediateDataSetID getOutputDataSetID() {
			return outputDataSetID;
		}

		public boolean hasOutputDataSetID() {
			return this.outputDataSetID != null;
		}

		public boolean hasInputDataSetID() {
			return this.inputDataSetID != null;
		}

		private int precomputeHash() {
			// TODO: is this safe?
			int hash = this.subTaskIndex;

			if (this.inputDataSetID != null) {
				hash ^= this.inputDataSetID.hashCode();
			}

			if (this.outputDataSetID != null) {
				hash ^= this.outputDataSetID.hashCode();
			}

			return hash;
		}

		public boolean isDummy() {
			return this.inputDataSetID == null || this.outputDataSetID == null;
		}

		@Override
		public int hashCode() {
			return this.precomputedHash;
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

			Vertex other = (Vertex) obj;

			return new EqualsBuilder().append(this.subTaskIndex, other.subTaskIndex)
					.append(this.inputDataSetID, other.inputDataSetID)
					.append(this.outputDataSetID, other.outputDataSetID).isEquals();
		}

		@Override
		public String toString() {
			return String.format("Rep:%s-%d-%s",
					(inputDataSetID != null) ? inputDataSetID.toString() : "none",
					this.subTaskIndex,
					(outputDataSetID != null) ? outputDataSetID.toString() : "none");
		}
	}

	public static class Edge extends QosReporterID {

		private final IntermediateResultPartitionID intermediateResultPartitionID;

		private final int consumedSubpartitionIndex;

		public Edge(IntermediateResultPartitionID intermediateResultPartitionID, int consumedSubpartitionIndex) {
			this.intermediateResultPartitionID = intermediateResultPartitionID;
			this.consumedSubpartitionIndex = consumedSubpartitionIndex;
		}

		public IntermediateResultPartitionID getIntermediateResultPartitionID() {
			return this.intermediateResultPartitionID;
		}

		public int getConsumedSubpartitionIndex() {
			return consumedSubpartitionIndex;
		}

		@Override
		public int hashCode() {
			return this.intermediateResultPartitionID.hashCode() ^ this.consumedSubpartitionIndex;
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
			Edge other = (Edge) obj;

			return this.intermediateResultPartitionID.equals(other.intermediateResultPartitionID)
					&& this.consumedSubpartitionIndex == other.consumedSubpartitionIndex;
		}

		@Override
		public String toString() {
			return String.format("Rep:%s-%d", this.intermediateResultPartitionID.toString(), this.consumedSubpartitionIndex);
		}
	}

	@Override
	public abstract int hashCode();

	@Override
	public abstract boolean equals(Object other);

	public static QosReporterID.Vertex forVertex(StreamTask task, VertexQosReporterConfig config) {
		return new Vertex(task.getIndexInSubtaskGroup(),
				config.getInputDataSetID(), config.getOutputDataSetID());
	}

	public static QosReporterID.Vertex forVertex(int subTaskIndex, VertexQosReporterConfig config) {
		return new Vertex(subTaskIndex,	config.getInputDataSetID(), config.getOutputDataSetID());
	}

	public static QosReporterID.Edge forEdge(
			IntermediateResultPartitionID intermediateResultPartitionID,
			int consumedSubpartitionIndex) {

		return new Edge(intermediateResultPartitionID, consumedSubpartitionIndex);
	}
}
