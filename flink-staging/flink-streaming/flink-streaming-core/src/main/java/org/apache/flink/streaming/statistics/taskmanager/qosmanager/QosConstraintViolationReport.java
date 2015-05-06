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

package org.apache.flink.streaming.statistics.taskmanager.qosmanager;

import org.apache.flink.streaming.statistics.JobGraphLatencyConstraint;
import org.apache.flink.streaming.statistics.LatencyConstraintID;

public class QosConstraintViolationReport {

	private LatencyConstraintID constraintID;

	private long latencyConstraintMillis;

	private double minSequenceLatency = Double.MAX_VALUE;;

	private double maxSequenceLatency = Double.MIN_VALUE;

	private double aggSequenceLatency = 0;

	private int noOfSequences = 0;

	private int noOfSequencesBelowConstraint = 0;

	private int noOfSequencesAboveConstraint = 0;

	private boolean isFinalized = false;
	
	public QosConstraintViolationReport() {
	}

	public QosConstraintViolationReport(JobGraphLatencyConstraint constraint) {
		this(constraint.getID(), constraint.getLatencyConstraintInMillis());
	}

	public QosConstraintViolationReport(LatencyConstraintID constraintID,
			long latencyConstraintMillis) {

		this.constraintID = constraintID;
		this.latencyConstraintMillis = latencyConstraintMillis;
	}

	public LatencyConstraintID getLatencyConstraintID() {
		return constraintID;
	}

	public long getLatencyConstraintMillis() {
		return latencyConstraintMillis;
	}

	public LatencyConstraintID getConstraintID() {
		return constraintID;
	}

	public double getMinSequenceLatency() {
		return minSequenceLatency;
	}

	public double getMeanSequenceLatency() {
		ensureIsFinalized();
		return aggSequenceLatency;
	}

	public double getMaxSequenceLatency() {
		return maxSequenceLatency;
	}

	public int getNoOfSequences() {
		return noOfSequences;
	}

	public int getNoOfSequencesBelowConstraint() {
		return noOfSequencesBelowConstraint;
	}

	public int getNoOfSequencesAboveConstraint() {
		return noOfSequencesAboveConstraint;
	}

	public void addQosSequenceLatencySummary(
			QosSequenceLatencySummary sequenceSummary) {

		if (this.isFinalized) {
			throw new RuntimeException(
					"Cannot add sequence to already finalized summary. This is a bug.");
		}

		double sequenceLatency = sequenceSummary.getSequenceLatency();
		this.aggSequenceLatency += sequenceLatency;

		if (sequenceLatency < this.minSequenceLatency) {
			this.minSequenceLatency = sequenceLatency;
		}

		if (sequenceLatency > this.maxSequenceLatency) {
			this.maxSequenceLatency = sequenceLatency;
		}

		this.noOfSequences++;

		double constraintViolatedByMillis = sequenceLatency
				- latencyConstraintMillis;

		// only count violations of >5% of the constraint
		if (Math.abs(constraintViolatedByMillis) / latencyConstraintMillis > 0.05) {
			if (constraintViolatedByMillis > 0) {
				this.noOfSequencesAboveConstraint++;
			} else {
				this.noOfSequencesBelowConstraint++;
			}
		}
	}
	
	public void merge(QosConstraintViolationReport other) {
		if(!constraintID.equals(other.constraintID)) {
			throw new RuntimeException("Cannot merge violation reports belonging to different constraints. This is bug.");
		}
		
		if (this.isFinalized) {
			throw new RuntimeException(
					"Cannot merge with into a finalized summary. This is a bug.");
		}
		
		
		this.aggSequenceLatency += other.noOfSequences * other.aggSequenceLatency;

		if (other.minSequenceLatency < this.minSequenceLatency) {
			this.minSequenceLatency = other.minSequenceLatency;
		}

		if (other.maxSequenceLatency > this.maxSequenceLatency) {
			this.maxSequenceLatency = other.maxSequenceLatency;
		}

		this.noOfSequences += other.noOfSequences;
	}

	private void ensureIsFinalized() {
		if (!this.isFinalized) {
			if (this.noOfSequences > 0) {
				this.aggSequenceLatency /= this.noOfSequences;
			} else {
				this.aggSequenceLatency = 0;
				this.minSequenceLatency = 0;
				this.maxSequenceLatency = 0;
				this.noOfSequencesAboveConstraint = 0;
				this.noOfSequencesBelowConstraint = 0;
			}
		}
		this.isFinalized = true;
	}
}
