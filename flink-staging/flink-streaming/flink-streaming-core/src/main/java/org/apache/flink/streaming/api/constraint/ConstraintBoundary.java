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

/**
 * Represents an edge in the stream graph marking begin OR end of a latency constraint,
 * e.g. one instance of ConstraintBoundary is needed for the beginning edge and one for the ending edge.
 */
public class ConstraintBoundary {

	private Integer sourceId;
	private Integer targetId;
	private int inputGate;
	private int outputGate;

	public ConstraintBoundary() {
		inputGate = -1;
		outputGate = -1;
	}

	/**
	 * The vertex id of the edge source.
	 */
	public Integer getSourceId() {
		return sourceId;
	}

	public void setSourceId(Integer sourceId) {
		this.sourceId = sourceId;
	}

	/**
	 * The vertex id of the edge target.
	 */
	public Integer getTargetId() {
		return targetId;
	}

	public void setTargetId(Integer targetId) {
		this.targetId = targetId;
	}

	public int getInputGate() {
		return inputGate;
	}

	public void setInputGate(int inputGate) {
		this.inputGate = inputGate;
	}

	public int getOutputGate() {
		return outputGate;
	}

	public void setOutputGate(int outputGate) {
		this.outputGate = outputGate;
	}
}
