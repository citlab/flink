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

import org.apache.flink.streaming.statistics.SequenceElement;

/**
 * Callback interface used by {@link QosGraphTraversal} to signal that a vertex
 * or edge is being traversed.
 *
 * @author Bjoern Lohrmann
 *
 */
public interface QosGraphTraversalListener {

	/**
	 * Callback to be implemented when depth first traversing a QosGraph along a
	 * {@link org.apache.flink.streaming.statistics.JobGraphSequence}. See
	 * {@link QosVertex#depthFirstTraverseForward()}.
	 *
	 * @param vertex
	 *            The current vertex during depth first traversal.
	 * @param sequenceElem
	 *            The current element of the sequence that directs the graph
	 *            traversal.
	 */
	public void processQosVertex(QosVertex vertex,
			SequenceElement sequenceElem);

	/**
	 * Callback to be implemented when depth first traversing a QosGraph along a
	 * {@link org.apache.flink.streaming.statistics.JobGraphSequence}. See
	 * {@link QosVertex#depthFirstTraverseForward()}.
	 *
	 * @param edge
	 *            The current edge during depth first traversal.
	 * @param sequenceElem
	 *            The current element of the sequence that directs the graph
	 *            traversal.
	 */
	public void processQosEdge(QosEdge edge,
			SequenceElement sequenceElem);

}
