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

package org.apache.flink.streaming.statistics.message.qosreport;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosReporterID;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.sampling.Sample;

import java.io.IOException;

/**
 * This class stores information about the latency (interread time), record
 * interarrival time as well as record consumption and emission rate of a vertex
 * (task).
 *
 * @author warneke, Bjoern Lohrmann
 */
public final class VertexStatistics extends AbstractQosReportRecord {

	private QosReporterID.Vertex reporterID;
	private Sample igInterReadTimeMillis;
	private double recordsConsumedPerSec;
	private double recordsEmittedPerSec;
	private boolean igIsChained;
	private Sample recordInterArrivalTimeMillis;

	public VertexStatistics(QosReporterID.Vertex reporterID,
			Sample igInterReadTimeMillis, double recordsConsumedPerSec,
			double recordsEmittedPerSec, boolean igIsChained,
			Sample recordInterArrivalTimeMillis) {

		this.reporterID = reporterID;
		this.igInterReadTimeMillis = igInterReadTimeMillis;
		this.recordsConsumedPerSec = recordsConsumedPerSec;
		this.recordsEmittedPerSec = recordsEmittedPerSec;
		this.recordInterArrivalTimeMillis = recordInterArrivalTimeMillis;
		this.igIsChained = recordInterArrivalTimeMillis == null;
	}

	public VertexStatistics(QosReporterID.Vertex reporterID,
			Sample igInterReadTimeMillis, double recordsConsumedPerSec,
			double recordsEmittedPerSec, Sample recordInterArrivalTimeMillis) {

		this(reporterID, igInterReadTimeMillis, recordsConsumedPerSec,
				recordsEmittedPerSec, false, recordInterArrivalTimeMillis);
	}

	public VertexStatistics(QosReporterID.Vertex reporterID,
			double recordsEmittedPerSec) {

		this(reporterID, null, -1, recordsEmittedPerSec, false, null);
	}

	public VertexStatistics(QosReporterID.Vertex reporterID,
			Sample readReadTimeMillis, double recordsConsumedPerSec,
			Sample recordInterArrivalTimeMillis) {

		this(reporterID, readReadTimeMillis, recordsConsumedPerSec, -1,
				false, recordInterArrivalTimeMillis);
	}

	/**
	 * Default constructor for the deserialization of the object.
	 */
	public VertexStatistics() {
	}

	public QosReporterID.Vertex getReporterID() {
		return this.reporterID;
	}

	/**
	 * Returns the time between a successful read on the reporter's input gate
	 * and the next attempt to read from any input gate.
	 *
	 * @return a sample with mean and variance
	 */
	public Sample getInputGateInterReadTimeMillis() {
		return this.igInterReadTimeMillis;
	}

	public double getRecordsConsumedPerSec() {
		return recordsConsumedPerSec;
	}

	public double getRecordsEmittedPerSec() {
		return recordsEmittedPerSec;
	}

	public boolean igIsChained() {
		return igIsChained;
	}

	public Sample getInterArrivalTimeMillis() {
		return recordInterArrivalTimeMillis;
	}

	public VertexStatistics fuseWith(VertexStatistics other) {

		boolean hasInputGate = reporterID.hasInputDataSetID();
		boolean hasOutputGate = reporterID.hasOutputDataSetID();

		VertexStatistics fused = new VertexStatistics(reporterID,
				igInterReadTimeMillis, recordsConsumedPerSec,
				recordsEmittedPerSec, igIsChained, recordInterArrivalTimeMillis);

		if (hasInputGate) {
			if (!igIsChained() && !other.igIsChained()) {
				fused.igIsChained = igIsChained();
				fused.recordInterArrivalTimeMillis = recordInterArrivalTimeMillis
						.fuseWithDisjunctSample(other.getInterArrivalTimeMillis());
			} else if (!igIsChained()) {
				fused.igIsChained = igIsChained();
				fused.recordInterArrivalTimeMillis = recordInterArrivalTimeMillis;
			} else if (!other.igIsChained()) {
				fused.igIsChained = other.igIsChained();
				fused.recordInterArrivalTimeMillis = other.getInterArrivalTimeMillis();
			}

			fused.igInterReadTimeMillis = igInterReadTimeMillis
					.fuseWithDisjunctSample(other
							.getInputGateInterReadTimeMillis());

			fused.recordsConsumedPerSec = (recordsConsumedPerSec + other
					.getRecordsConsumedPerSec()) / 2;
		}

		if (hasOutputGate) {
			fused.recordsEmittedPerSec = (recordsEmittedPerSec + other
					.getRecordsEmittedPerSec()) / 2;
		}

		return fused;
	}

	@Override
	public void write(final DataOutputView out) throws IOException {
		this.reporterID.write(out);
		out.writeBoolean(this.igIsChained);

		boolean hasInputGate = reporterID.hasInputDataSetID();
		boolean hasOutputGate = reporterID.hasOutputDataSetID();

		if (hasInputGate) {
			if (!igIsChained) {
				recordInterArrivalTimeMillis.write(out);
			}
			igInterReadTimeMillis.write(out);
			out.writeDouble(this.getRecordsConsumedPerSec());
		}

		if (hasOutputGate) {
			out.writeDouble(this.getRecordsEmittedPerSec());
		}
	}

	@Override
	public void read(final DataInputView in) throws IOException {
		this.reporterID = new QosReporterID.Vertex();
		this.reporterID.read(in);
		this.igIsChained = in.readBoolean();

		boolean hasInputGate = reporterID.hasInputDataSetID();
		boolean hasOutputGate = reporterID.hasOutputDataSetID();

		if (hasInputGate) {
			if (!igIsChained) {
				recordInterArrivalTimeMillis = new Sample();
				recordInterArrivalTimeMillis.read(in);
			}
			igInterReadTimeMillis = new Sample();
			igInterReadTimeMillis.read(in);
			this.recordsConsumedPerSec = in.readDouble();
		}

		if (hasOutputGate) {
			this.recordsEmittedPerSec = in.readDouble();
		}
	}
}
