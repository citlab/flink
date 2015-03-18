/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.constraint;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.StreamGraph;

/**
 * An entity of the {@link StreamGraph} being either a vertex or a edge
 */
public class StreamSequenceElement implements IOReadableWritable {
	private boolean isVertex;

	private int vertexID;

	private int targetVertexId;
	private int targetIndex;


	public StreamSequenceElement() {
	}

	/**
	 * Creates a vertex sequence element.
	 *
	 * @param vertexID
	 * 		the id of the vertex;
	 */
	public StreamSequenceElement(int vertexID) {
		this.vertexID = vertexID;
		this.isVertex = true;
	}

	/**
	 * Create a edge sequence element.
	 *
	 * @param sourceVertexID
	 * 		id of the source vertex.
	 * @param targetVertexId
	 * 		id of the target vertex.
	 * @param targetIndex
	 * 		the index of the target vertex in the out edge list of the source vertex.
	 */
	public StreamSequenceElement(int sourceVertexID, int targetVertexId, int targetIndex) {
		this.vertexID = sourceVertexID;
		this.targetVertexId = targetVertexId;
		this.targetIndex = targetIndex;
	}

	public boolean isVertex() {
		return isVertex;
	}

	public int getVertexID() {
		return vertexID;
	}

	public int getTargetVertexId() {
		return targetVertexId;
	}

	public int getTargetIndex() {
		return targetIndex;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeBoolean(isVertex);
		out.writeInt(vertexID);
		if (isVertex) {
			out.writeInt(targetVertexId);
			out.writeInt(targetIndex);
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		isVertex = in.readBoolean();
		vertexID = in.readInt();
		if (isVertex) {
			targetVertexId = in.readInt();
			targetIndex = in.readInt();
		}
	}
}
