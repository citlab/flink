/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.streaming.api.constraint;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * An entity of the {@link org.apache.flink.streaming.api.graph.StreamGraph} being either a vertex or a edge
 */
public class JobGraphSequenceElement implements IOReadableWritable {

	private boolean isVertex;

	private JobVertexID vertexId;
	private JobVertexID targetId;
	private int targetIndex;

	public JobGraphSequenceElement() {
	}

	/**
	 * Creates a vertex sequence element.
	 *
	 * @param vertexId
	 * 		the id of the vertex;
	 */
	public JobGraphSequenceElement(JobVertexID vertexId) {
		this.vertexId = vertexId;
		isVertex = true;
	}

	/**
	 * Create a edge sequence element.
	 *  @param vertexId
	 * 		id of the source vertex.
	 * @param targetId
	 * 		id of the target vertex.
	 * @param targetIndex
	 * 		index of the intermediate data set.
	 */
	public JobGraphSequenceElement(JobVertexID vertexId, JobVertexID targetId, int targetIndex) {
		this.vertexId = vertexId;
		this.targetId = targetId;
		this.targetIndex = targetIndex;
	}


	public JobVertexID getVertexId() {
		return vertexId;
	}

	public JobVertexID getTargetId() {
		return targetId;
	}

	public int getTargetIndex() {
		return targetIndex;
	}

	public boolean isVertex() {
		return isVertex;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeBoolean(isVertex);
		vertexId.write(out);
		if (!isVertex) {
			targetId.write(out);
			out.writeInt(targetIndex);
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		boolean isVertex = in.readBoolean();
		vertexId = new JobVertexID();
		vertexId.read(in);
		if (!isVertex) {
			targetId = new JobVertexID();
			targetId.read(in);
			targetIndex = in.readInt();
		}
	}
}