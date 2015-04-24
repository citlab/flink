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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.statistics.SamplingStrategy;
import org.apache.flink.streaming.statistics.SequenceElement;

import java.io.IOException;

/**
 * Describes a Qos reporter role for a vertex (task).
 *
 * @author Bjoern Lohrmann, Sascha Wolke
 */
public class VertexQosReporterConfig implements QosReporterConfig {

	private JobVertexID groupVertexID;

//	private ExecutionAttemptID executionAttemptID;

	private int inputGateIndex;

	private IntermediateDataSetID inputDataSetID;

	private int outputGateIndex;

	private IntermediateDataSetID outputDataSetID;

	private SamplingStrategy samplingStrategy;

//	private int subTaskIndex;

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
								   // ExecutionAttemptID executionAttemptID,
			int inputGateIndex, IntermediateDataSetID inputDataSetID,
			int outputGateIndex, IntermediateDataSetID outputDataSetID,
			SamplingStrategy samplingStrategy,
//			int subTaskIndex,
			String name) {

		if (inputGateIndex == -1 ^ inputDataSetID == null || outputGateIndex == -1 ^ outputDataSetID == null) {

			throw new RuntimeException(
					"If inputGateIndex/outputGateIndex is (not) -1, the respective data set ID must (not) be null. This is a bug.");
		}

		this.groupVertexID = groupVertexID;
//		this.executionAttemptID = executionAttemptID;
		this.inputGateIndex = inputGateIndex;
		this.inputDataSetID= inputDataSetID;
		this.outputGateIndex = outputGateIndex;
		this.outputDataSetID = outputDataSetID;
		this.samplingStrategy = samplingStrategy;
//		this.subTaskIndex = subTaskIndex;
		this.name = name;
	}

//	public boolean isDummy() {
//		return this.getReporterID().isDummy();
//	}

	public JobVertexID getGroupVertexID() {
		return this.groupVertexID;
	}

//	public ExecutionAttemptID getExecutionAttemptID() {
//		return executionAttemptID;
//	}

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

//	public int getSubTaskIndex() {
//		return subTaskIndex;
//	}

	public String getName() {
		return this.name;
	}

//	public QosVertex toQosVertex() {
//		return new QosVertex(this.vertexID, this.name, this.reporterInstance,
//				this.memberIndex);
//	}
//
//	public QosGate toInputGate() {
//		return new QosGate(this.inputGateID, this.inputGateIndex);
//	}
//
//	public QosGate toOutputGate() {
//		return new QosGate(this.outputGateID, this.outputGateIndex);
//	}

	@Override
	public void write(DataOutputView out) throws IOException {
		this.groupVertexID.write(out);
//		this.executionAttemptID.write(out);

		out.writeInt(this.inputGateIndex);
		if (this.inputGateIndex != -1) {
			this.inputDataSetID.write(out);
		}

		out.writeInt(this.outputGateIndex);
		if (this.outputGateIndex != -1) {
			this.outputDataSetID.write(out);
		}

//		if (!this.isDummy()) {
			out.writeUTF(samplingStrategy.toString());
//		}

//		out.writeInt(this.subTaskIndex);
		out.writeUTF(this.name);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.groupVertexID = new JobVertexID();
		this.groupVertexID.read(in);
//		this.executionAttemptID = new ExecutionAttemptID();
//		this.executionAttemptID.read(in);

		this.inputGateIndex = in.readInt();
		if (this.inputGateIndex != -1) {
			this.inputDataSetID = new IntermediateDataSetID();
			this.inputDataSetID.read(in);
		}

		this.outputGateIndex = in.readInt();
		if (this.outputGateIndex != -1) {
			this.outputDataSetID = new IntermediateDataSetID();
			this.outputDataSetID.read(in);
		}

//		if (!this.isDummy()) {
			this.samplingStrategy = SamplingStrategy.valueOf(in.readUTF());
//		}

//		this.subTaskIndex = in.readInt();
		this.name = in.readUTF();
	}

	@Override
	public String toString() {
		return "VertexQosReporterConfig("
				+ this.inputGateIndex + " -> " + this.name + " -> " + this.outputGateIndex
				+ ", groupVertex=" + this.groupVertexID + ", " + this.samplingStrategy + ")";
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
