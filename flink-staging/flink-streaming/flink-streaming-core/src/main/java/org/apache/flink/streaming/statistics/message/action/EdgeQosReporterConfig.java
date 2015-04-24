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
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosEdge;

import java.io.IOException;

/**
 * Describes a Qos reporter role for an edge.
 *
 * @author Bjoern Lohrmann, Sascha Wolke
 */
public class EdgeQosReporterConfig implements QosReporterConfig {

//	private IntermediateResultPartitionID intermediateResultPartitionID;

//	private int consumedSubpartitionIndex;

	private IntermediateDataSetID intermediateDataSetID;

	private int outputGateEdgeIndex; // optional (based on intermediate result)

	private int inputGateEdgeIndex; // optional (based on intermediate result)

	private String name;

	public EdgeQosReporterConfig() {
	}

	/**
	 * Initializes EdgeQosReporterConfig.
	 */
	public EdgeQosReporterConfig(IntermediateDataSetID intermediateDataSetID,
			int outputGateEdgeIndex, int inputGateEdgeIndex, String name) {

		this.intermediateDataSetID = intermediateDataSetID;
		this.outputGateEdgeIndex = outputGateEdgeIndex;
		this.inputGateEdgeIndex = inputGateEdgeIndex;
		this.name = name;
	}

	public IntermediateDataSetID getIntermediateDataSetID() {
		return intermediateDataSetID;
	}

	public int getOutputGateEdgeIndex() {
		return this.outputGateEdgeIndex;
	}

	public int getInputGateEdgeIndex() {
		return this.inputGateEdgeIndex;
	}

	public String getName() {
		return this.name;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		this.intermediateDataSetID.write(out);
		out.writeInt(this.outputGateEdgeIndex);
		out.writeInt(this.inputGateEdgeIndex);
		out.writeUTF(this.name);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.intermediateDataSetID = new IntermediateDataSetID();
		this.intermediateDataSetID.read(in);
		this.outputGateEdgeIndex = in.readInt();
		this.inputGateEdgeIndex = in.readInt();
		this.name = in.readUTF();
	}

	@Override
	public String toString() {
		return "EdgeQosReporterConfig("
				+ this.outputGateEdgeIndex + " -> " + this.name + " -> " + this.inputGateEdgeIndex
				+ ", dataSet: " + this.intermediateDataSetID + ")";
	}

	public QosEdge toQosEdge(IntermediateResultPartitionID partitionID, int subpartitionIndex) {
		return new QosEdge(partitionID, subpartitionIndex, this.outputGateEdgeIndex, this.inputGateEdgeIndex);
	}
}
