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
import org.apache.flink.streaming.statistics.JobGraphSequence;
import org.apache.flink.streaming.statistics.LatencyConstraintID;
import org.apache.flink.streaming.statistics.SequenceElement;

import java.util.LinkedList;
import java.util.List;

public class QosConstraintSummary {

	private QosGroupElementSummary[] groupElemSummaries;

	private QosConstraintViolationReport violationReport;

	public QosConstraintSummary() {
	}

	public QosConstraintSummary(JobGraphLatencyConstraint constraint,
			QosConstraintViolationReport violationReport) {

		this.violationReport = violationReport;
		initGroupElemSummaryArray(constraint);
	}

	public void initGroupElemSummaryArray(JobGraphLatencyConstraint constraint) {
		JobGraphSequence seq = constraint.getSequence();
		groupElemSummaries = new QosGroupElementSummary[seq.size()];

		int i = 0;
		for (SequenceElement seqElem : seq) {
			if (seqElem.isVertex()) {
				groupElemSummaries[i] = new QosGroupVertexSummary();
			} else {
				groupElemSummaries[i] = new QosGroupEdgeSummary();
			}
			i++;
		}
	}

	public QosGroupElementSummary[] QosGroupElementSummaries() {
		return groupElemSummaries;
	}

	public QosConstraintViolationReport getViolationReport() {
		return violationReport;
	}

	public boolean doesSequenceStartWithVertex() {
		return this.groupElemSummaries[0].isVertex();
	}

	public int getSequenceLength() {
		return this.groupElemSummaries.length;
	}

	public LatencyConstraintID getLatencyConstraintID() {
		return this.violationReport.getConstraintID();
	}

	public QosGroupVertexSummary getGroupVertexSummary(int indexInSequence) {
		return (QosGroupVertexSummary) groupElemSummaries[indexInSequence];
	}

	public QosGroupEdgeSummary getGroupEdgeSummary(int indexInSequence) {
		return (QosGroupEdgeSummary) groupElemSummaries[indexInSequence];
	}

	public void merge(List<QosConstraintSummary> completeSummaries) {
		// merge violation reports
		for (QosConstraintSummary summary : completeSummaries) {
			violationReport.merge(summary.getViolationReport());
		}

		// merge elem summaries groupwise
		List<QosGroupElementSummary> elemSummaries = new LinkedList<QosGroupElementSummary>();
		for (int i = 0; i < getSequenceLength(); i++) {
			for (QosConstraintSummary toMerge : completeSummaries) {
				elemSummaries.add(toMerge.groupElemSummaries[i]);
			}
			groupElemSummaries[i].merge(elemSummaries);
			elemSummaries.clear();
		}
	}

	public boolean hasData() {
		for (QosGroupElementSummary elemSumary : groupElemSummaries) {
			if (!elemSumary.hasData()) {
				return false;
			}
		}
		return true;
	}
}
