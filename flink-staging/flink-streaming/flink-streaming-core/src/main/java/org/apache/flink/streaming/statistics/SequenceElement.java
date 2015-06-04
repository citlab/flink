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

package org.apache.flink.streaming.statistics;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * A sequence is a series of connected vertices (tasks) and edges (channels).
 * This class models an element of such a sequence and thus models either a
 * vertex or an edge. To unambiguously define a sequence we needs to not only
 * include vertex IDs but also the indices of input/output gates.
 *
 * @author Bjoern Lohrmann
 */
public class SequenceElement implements	IOReadableWritable {

	private JobVertexID sourceVertexID;
	private JobVertexID targetVertexID;
	private int inputGateIndex;
	private int outputGateIndex;
	private boolean isVertex;
	private SamplingStrategy samplingStrategy;
	private int indexInSequence;
	private String name;

	public SequenceElement() {
	}

	public SequenceElement(JobVertexID vertexID, int inputGateIndex, int outputGateIndex, int indexInSequence, String name) {
		this(vertexID, inputGateIndex, outputGateIndex, indexInSequence, name, SamplingStrategy.READ_READ);
	}

	public SequenceElement(JobVertexID vertexID, int inputGateIndex, int outputGateIndex, int indexInSequence, String name,
			SamplingStrategy samplingStrategy) {
		this.sourceVertexID = vertexID;
		this.inputGateIndex = inputGateIndex;
		this.outputGateIndex = outputGateIndex;
		this.isVertex = true;
		this.samplingStrategy = samplingStrategy;
		this.indexInSequence = indexInSequence;
		this.name = name;
	}

	public SequenceElement(JobVertexID sourceVertexID, int outputGateIndex,
			JobVertexID targetVertexID, int inputGateIndex, int indexInSequence, String name) {
		this.sourceVertexID = sourceVertexID;
		this.targetVertexID = targetVertexID;
		this.inputGateIndex = inputGateIndex;
		this.outputGateIndex = outputGateIndex;
		this.isVertex = false;
		this.indexInSequence = indexInSequence;
		this.name = name;
	}

	public JobVertexID getVertexID() {
		return this.sourceVertexID;
	}

	public JobVertexID getSourceVertexID() {
		return this.sourceVertexID;
	}

	public JobVertexID getTargetVertexID() {
		return this.targetVertexID;
	}

	public int getInputGateIndex() {
		return this.inputGateIndex;
	}

	public int getOutputGateIndex() {
		return this.outputGateIndex;
	}

	public String getName() {
		return name;
	}

	public boolean isVertex() {
		return this.isVertex;
	}

	public SamplingStrategy getSamplingStrategy() {
		return samplingStrategy;
	}

	public void setSamplingStrategy(SamplingStrategy samplingStrategy) {
		this.samplingStrategy = samplingStrategy;
	}

	public boolean isEdge() {
		return !this.isVertex;
	}

	public int getIndexInSequence() {
		return this.indexInSequence;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeBoolean(this.isVertex);
		if (this.isVertex) {
			out.writeUTF(samplingStrategy.toString());
		}
		this.sourceVertexID.write(out);
		if (!this.isVertex) {
			this.targetVertexID.write(out);
		}
		out.writeInt(this.inputGateIndex);
		out.writeInt(this.outputGateIndex);
		out.writeInt(this.indexInSequence);
		out.writeUTF(this.name);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.isVertex = in.readBoolean();
		if (this.isVertex) {
			this.samplingStrategy = SamplingStrategy.valueOf(in.readUTF());
		}

		this.sourceVertexID = new JobVertexID();
		this.sourceVertexID.read(in);
		if (!this.isVertex) {
			this.targetVertexID = new JobVertexID();
			this.targetVertexID.read(in);
		}
		this.inputGateIndex = in.readInt();
		this.outputGateIndex = in.readInt();
		this.indexInSequence = in.readInt();
		this.name = in.readUTF();
	}
}
