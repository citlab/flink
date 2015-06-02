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

package org.apache.flink.streaming.statistics.message.action;

import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.statistics.SamplingStrategy;
import org.apache.flink.streaming.statistics.SequenceElement;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosGate;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Describes a Qos reporter role for a vertex (task).
 *
 * @author Bjoern Lohrmann, Sascha Wolke
 */
public class VertexQosReporterConfig implements QosReporterConfig {

	private JobVertexID groupVertexID;

	private int inputGateIndex;

	private IntermediateDataSetID inputDataSetID;

	private int outputGateIndex;

	private IntermediateDataSetID outputDataSetID;

	private SamplingStrategy samplingStrategy;

	private String name;

	public VertexQosReporterConfig() {
	}

	/**
	 * Initializes VertexQosReporterConfig. Dummy reporter configs can be
	 * created by providing -1 gate indices and null data set IDs.
	 *
	 * @param inputGateIndex
	 *            If inputGateIndex is -1, inputDataSetID must be null (and vice
	 *            versa).
	 * @param outputGateIndex
	 *            If outputGateIndex is -1, outputDataSetID must be null (and vice
	 *            versa).
	 */
	public VertexQosReporterConfig(JobVertexID groupVertexID,
			int inputGateIndex, IntermediateDataSetID inputDataSetID,
			int outputGateIndex, IntermediateDataSetID outputDataSetID,
			SamplingStrategy samplingStrategy,
			String name) {

		if (inputGateIndex == -1 ^ inputDataSetID == null || outputGateIndex == -1 ^ outputDataSetID == null) {

			throw new RuntimeException(
					"If inputGateIndex/outputGateIndex is (not) -1, the respective data set ID must (not) be null. This is a bug.");
		}

		this.groupVertexID = groupVertexID;
		this.inputGateIndex = inputGateIndex;
		this.inputDataSetID= inputDataSetID;
		this.outputGateIndex = outputGateIndex;
		this.outputDataSetID = outputDataSetID;
		this.samplingStrategy = samplingStrategy;
		this.name = name;
	}

	public JobVertexID getGroupVertexID() {
		return this.groupVertexID;
	}

	public int getInputGateIndex() {
		return this.inputGateIndex;
	}

	public IntermediateDataSetID getInputDataSetID() {
		return inputDataSetID;
	}

	public int getOutputGateIndex() {
		return this.outputGateIndex;
	}

	public IntermediateDataSetID getOutputDataSetID() {
		return outputDataSetID;
	}

	public SamplingStrategy getSamplingStrategy() {
		return samplingStrategy;
	}

	public String getName() {
		return this.name;
	}

	public QosGate toInputGate() {
		return new QosGate(QosGate.GateType.INPUT_GATE, this.inputDataSetID, this.inputGateIndex);
	}

	public QosGate toOutputGate() {
		return new QosGate(QosGate.GateType.OUTPUT_GATE, this.outputDataSetID, this.outputGateIndex);
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeObject(this.groupVertexID);

		out.writeInt(this.inputGateIndex);
		if (this.inputGateIndex != -1) {
			out.writeObject(this.inputDataSetID);
		}

		out.writeInt(this.outputGateIndex);
		if (this.outputGateIndex != -1) {
			out.writeObject(this.outputDataSetID);
		}

//		if (!this.isDummy()) {
			out.writeUTF(samplingStrategy.toString());
//		}

		out.writeUTF(this.name);
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		this.groupVertexID = (JobVertexID) in.readObject();

		this.inputGateIndex = in.readInt();
		if (this.inputGateIndex != -1) {
			this.inputDataSetID = (IntermediateDataSetID) in.readObject();
		}

		this.outputGateIndex = in.readInt();
		if (this.outputGateIndex != -1) {
			this.outputDataSetID = (IntermediateDataSetID) in.readObject();
		}

//		if (!this.isDummy()) {
			this.samplingStrategy = SamplingStrategy.valueOf(in.readUTF());
//		}

		this.name = in.readUTF();
	}

	@Override
	public String toString() {
		return "VertexQosReporterConfig("
				+ this.inputGateIndex + " -> " + this.name + " -> " + this.outputGateIndex
				+ ", groupVertex=" + this.groupVertexID + ", " + this.samplingStrategy + ")";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof VertexQosReporterConfig) {
			VertexQosReporterConfig other = (VertexQosReporterConfig) obj;
			return this.groupVertexID.equals(other.groupVertexID)
					&& this.inputGateIndex == other.inputGateIndex
					&& this.inputDataSetID.equals(other.inputDataSetID)
					&& this.outputGateIndex == other.outputGateIndex
					&& this.outputDataSetID.equals(other.outputDataSetID);

		} else {
			return false;
		}
	}

	public static VertexQosReporterConfig fromSequenceElement(AbstractJobVertex vertex, SequenceElement element) {
		if (element.getInputGateIndex() >= 0 && element.getOutputGateIndex() >= 0) {
			return new VertexQosReporterConfig(
					vertex.getID(),
					element.getInputGateIndex(), vertex.getInputs().get(element.getInputGateIndex()).getSourceId(),
					element.getOutputGateIndex(), vertex.getProducedDataSets().get(element.getOutputGateIndex()).getId(),
					element.getSamplingStrategy(), element.getName());

		} else if (element.getInputGateIndex() >= 0) {
			return new VertexQosReporterConfig(
					vertex.getID(),
					element.getInputGateIndex(), vertex.getInputs().get(element.getInputGateIndex()).getSourceId(),
					-1, null,
					element.getSamplingStrategy(), element.getName());

		} else {
			return new VertexQosReporterConfig(
					vertex.getID(),
					-1, null,
					element.getOutputGateIndex(), vertex.getProducedDataSets().get(element.getOutputGateIndex()).getId(),
					element.getSamplingStrategy(), element.getName());
		}
	}
}
