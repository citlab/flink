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

import java.util.LinkedList;
import java.util.List;

import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;

/**
 * A helper class for finding path in a {@link StreamGraph} between two vertex ids.
 */
public class StreamGraphSequenceFinder {
	private StreamGraph streamGraph;

	public StreamGraphSequenceFinder(StreamGraph streamGraph) {
		this.streamGraph = streamGraph;
	}

	/**
	 * Finds all sequences between two vertex ids.
	 *
	 * @param beginVertexId
	 * 		the vertex id the sequences should begin with.
	 * @param endVertexId
	 * 		the vertex id the sequences should end with.
	 * @return a list of all found sequences.
	 */
	public List<StreamGraphSequence> findAllSequencesBetween(int beginVertexId, int endVertexId) {

		StreamGraphSequence stack = new StreamGraphSequence();
		LinkedList<StreamGraphSequence> result = new LinkedList<StreamGraphSequence>();

		depthFirstSequenceEnumerate(beginVertexId, stack, result, endVertexId);

		return result;
	}

	private void depthFirstSequenceEnumerate(
			int currentVertexId, StreamGraphSequence stack, LinkedList<StreamGraphSequence> result, int endVertexId) {

		stack.add(new StreamGraphSequenceElement(currentVertexId));

		if (currentVertexId == endVertexId) {
			result.add(((StreamGraphSequence) stack.clone()));
		} else {
			List<StreamEdge> outEdges = streamGraph.getVertex(currentVertexId).getOutEdges();

			for (int i = 0; i < outEdges.size(); i++) {
				int targetId = outEdges.get(i).getTargetID();
				StreamGraphSequenceElement edge = new StreamGraphSequenceElement(currentVertexId, targetId, i);
				stack.add(edge);
				depthFirstSequenceEnumerate(targetId, stack, result, endVertexId);
				stack.removeLast();
			}
		}

		stack.removeLast();
	}
}
