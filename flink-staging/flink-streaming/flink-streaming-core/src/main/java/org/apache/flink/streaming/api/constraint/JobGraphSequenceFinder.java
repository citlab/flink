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

import java.util.LinkedList;
import java.util.List;

import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;

public class JobGraphSequenceFinder {
	private JobGraph jobGraph;

	public JobGraphSequenceFinder(JobGraph jobGraph) {
		this.jobGraph = jobGraph;
	}

	/**
	 * Return a list of all possible {@link JobGraphSequence}s between two job vertices.
	 *
	 * @param beginId
	 * 		the id of the begin vertex.
	 * @param endId
	 * 		the id of the end vertex.
	 * @return list of job graph sequences.
	 */
	public List<JobGraphSequence> findAllSequencesBetween(JobVertexID beginId, JobVertexID endId) {
		AbstractJobVertex beginVertex = jobGraph.findVertexByID(beginId);
		beginVertex.getProducedDataSets();

		JobGraphSequence stack = new JobGraphSequence();
		LinkedList<JobGraphSequence> result = new LinkedList<JobGraphSequence>();

		depthFirstSequenceEnumerate(beginId, stack, result, endId);

		return result;
	}

	private void depthFirstSequenceEnumerate(JobVertexID currentVertexId, JobGraphSequence stack,
			LinkedList<JobGraphSequence> result, JobVertexID endVertexId) {

		stack.add(new JobGraphSequenceElement(currentVertexId));

		if (currentVertexId == endVertexId) {
			result.add((JobGraphSequence) stack.clone());
		} else {
			List<IntermediateDataSet> dataSets = jobGraph.findVertexByID(currentVertexId).getProducedDataSets();
			for (int i = 0; i < dataSets.size(); i++) {
				// uses assumptions about how a stream graph is converted into a job graph
				JobGraphSequenceElement edge =
						new JobGraphSequenceElement(currentVertexId, dataSets.get(i).getConsumers().get(0).getTarget().getID(), i);
				stack.add(edge);
				depthFirstSequenceEnumerate(edge.getTargetId(), stack, result, endVertexId);
				stack.removeLast();
			}
		}

		stack.removeLast();
	}
}