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

import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosReporterID;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * This class holds statistical information about an edge (output channel side),
 * such as throughput, output buffer lifetime, records per buffer and records
 * per second.
 *
 * @author Bjoern Lohrmann
 */
public final class EdgeStatistics extends AbstractQosReportRecord {

	/**
	 * The ID of reporter.
	 */
	private QosReporterID.Edge reporterID;

	/**
	 * The throughput in MBit/s.
	 */
	private double throughput;

	/**
	 * The lifetime of an output buffer on this specific output channel in
	 * millis.
	 */
	private double outputBufferLifetime;

	/**
	 * The number of records that fit into an output buffer of this channel.
	 */
	private double recordsPerBuffer;

	/**
	 * The number of records per second the task emits on the channel.
	 */
	private double recordsPerSecond;

	/**
	 * Default constructor for deserialization.
	 */
	public EdgeStatistics() {
	}

	/**
	 * /** Constructs a new channel throughput object.
	 *
	 * @param reporterID
	 *            the ID of the QOs reporter
	 * @param throughput
	 *            throughput of the output channel in MBit/s
	 * @param outputBufferLifetime
	 *            lifetime of an output buffer on this specific output channel
	 *            in millis
	 * @param recordsPerBuffer
	 *            number of records per output buffer on this channel
	 * @param recordsPerSecond
	 *            number of records that are emitted on this channel each second
	 */
	public EdgeStatistics(QosReporterID.Edge reporterID, double throughput,
			double outputBufferLifetime, double recordsPerBuffer,
			double recordsPerSecond) {

		this.reporterID = reporterID;
		this.throughput = throughput;
		this.outputBufferLifetime = outputBufferLifetime;
		this.recordsPerBuffer = recordsPerBuffer;
		this.recordsPerSecond = recordsPerSecond;

	}

	/**
	 * Returns the throughput of the output channel in MBit/s.
	 *
	 * @return the throughput of the output channel in MBit/s.
	 */
	public double getThroughput() {
		return this.throughput;
	}

	/**
	 * Returns the lifetime of an output buffer on this specific output channel
	 * in millis.
	 *
	 * @return the lifetime of an output buffer on this specific output channel
	 *         in millis.
	 */
	public double getOutputBufferLifetime() {
		return this.outputBufferLifetime;
	}

	/**
	 * Returns the number of records that fit into an output buffer of this
	 * channel.
	 *
	 * @return the number of records that fit into an output buffer of this
	 *         channel.
	 */
	public double getRecordsPerBuffer() {
		return this.recordsPerBuffer;
	}

	/**
	 * Returns the number of records per second the task emits on the channel.
	 *
	 * @return the number of records per second the task emits on the channel.
	 */
	public double getRecordsPerSecond() {
		return this.recordsPerSecond;
	}

	@Override
	public QosReporterID.Edge getReporterID() {
		return this.reporterID;
	}

	public EdgeStatistics fuseWith(EdgeStatistics other) {
		return new EdgeStatistics(reporterID,
				(throughput + other.throughput) / 2,
				(outputBufferLifetime + other.outputBufferLifetime) / 2,
				(recordsPerBuffer + other.recordsPerBuffer) / 2,
				(recordsPerSecond + other.recordsPerSecond) / 2);
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeObject(this.reporterID);
		out.writeDouble(this.getThroughput());
		out.writeDouble(this.getOutputBufferLifetime());
		out.writeDouble(this.getRecordsPerBuffer());
		out.writeDouble(this.getRecordsPerSecond());
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		this.reporterID = (QosReporterID.Edge) in.readObject();
		this.throughput = in.readDouble();
		this.outputBufferLifetime = in.readDouble();
		this.recordsPerBuffer = in.readDouble();
		this.recordsPerSecond = in.readDouble();
	}
}
