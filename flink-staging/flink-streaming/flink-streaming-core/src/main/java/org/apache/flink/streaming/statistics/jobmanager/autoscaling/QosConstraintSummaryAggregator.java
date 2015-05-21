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

package org.apache.flink.streaming.statistics.jobmanager.autoscaling;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.streaming.statistics.JobGraphLatencyConstraint;
import org.apache.flink.streaming.statistics.SequenceElement;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.QosConstraintSummary;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.QosConstraintViolationReport;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.QosGroupEdgeSummary;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.QosGroupVertexSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * This class hold only one summary now, which might be outdated.
 * Due to the fact that we have only one qos manager now, this class is useless.
 * TODO: remove this class
 */
public class QosConstraintSummaryAggregator {
	
	private static final Logger LOG = LoggerFactory.getLogger(QosConstraintSummaryAggregator.class);
	
	private QosConstraintSummary summary;

	private final ExecutionGraph executionGraph;

	private final JobGraphLatencyConstraint constraint;

	public QosConstraintSummaryAggregator(ExecutionGraph executionGraph, JobGraphLatencyConstraint constraint) {

		this.executionGraph = executionGraph;
		this.constraint = constraint;
	}
	
	public JobGraphLatencyConstraint getConstraint() {
		return constraint;
	}

	public void add(QosConstraintSummary summary) {
		this.summary = summary;
	}
	
	public boolean canAggregate() {
		return this.summary.hasData();
	}

	/**
	 * This method is called only if current summary has data (@see #canAggregate()).
	 */
	public QosConstraintSummary computeAggregation() {
		fixNoOfActiveVerticesAndEdges(this.summary);
		return this.summary;
	}

	public void fixNoOfActiveVerticesAndEdges(QosConstraintSummary aggregation) {
		for(SequenceElement seqElem : constraint.getSequence()) {
			if(seqElem.isVertex()) {
				int trueNoOfSubtasks = this.executionGraph
						.getJobVertex(seqElem.getVertexID())
						.getParallelism();

				QosGroupVertexSummary vertexSum = aggregation.getGroupVertexSummary(seqElem.getIndexInSequence());
				if(trueNoOfSubtasks > vertexSum.getActiveVertices()) {
					LOG.warn(String
							.format("Aggregation for \"%s\" does not provide enough data",
									seqElem.getName()));
				}
			
				vertexSum.setActiveVertices(trueNoOfSubtasks);
			} else {
				int trueNoOfEmitterSubtasks = this.executionGraph
						.getJobVertex(seqElem.getSourceVertexID())
						.getParallelism();

				int trueNoOfConsumerSubtasks = this.executionGraph
						.getJobVertex(seqElem.getTargetVertexID())
						.getParallelism();

				QosGroupEdgeSummary edgeSum = aggregation
						.getGroupEdgeSummary(seqElem.getIndexInSequence());
				
				if (trueNoOfEmitterSubtasks > edgeSum.getActiveEmitterVertices()
						|| trueNoOfConsumerSubtasks > edgeSum.getActiveConsumerVertices()) {
					
					LOG.warn(String
							.format("Aggregation for edge \"%s\" does not provide enough data",
									seqElem.getName() + seqElem.getIndexInSequence()));
				}
				
				edgeSum.setActiveEmitterVertices(trueNoOfEmitterSubtasks);
				edgeSum.setActiveConsumerVertices(trueNoOfConsumerSubtasks);
				edgeSum.setActiveEdges(-1);
			}
		}
	}
}
