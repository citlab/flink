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

import java.util.List;

import org.apache.flink.streaming.statistics.JobGraphSequence;

/**
 * Represents a constraint group on a {@link org.apache.flink.runtime.jobgraph.JobGraph}.
 * It consists of the {@link JobGraphSequence}s which are affected
 * by the constraint and a maximum latency.
 */
public class JobGraphConstraintGroup {
	private List<JobGraphSequence> sequences;
	private long maxLatency;


	public JobGraphConstraintGroup() {
	}

	public JobGraphConstraintGroup(List<JobGraphSequence> sequences, long maxLatency) {
		this.sequences = sequences;
		this.maxLatency = maxLatency;
	}

	/**
	 * @return the sequences affected by the constraint.
	 */
	public List<JobGraphSequence> getSequences() {
		return sequences;
	}

	/**
	 * @return the desired maximum latency of the constraint.
	 */
	public long getMaxLatency() {
		return maxLatency;
	}

}