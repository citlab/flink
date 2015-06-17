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
package org.apache.flink.streaming.statistics.taskmanager.qosreporter.listener;

import org.apache.flink.streaming.runtime.io.StreamRecordWriter;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.InputGateReporterManager;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.OutputGateReporterManager;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.QosReportingReader;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.vertex.VertexStatisticsReportManager;
import org.apache.flink.streaming.statistics.types.TimeStampedRecord;

/**
 * Utility class that creates {@link InputGateQosReportingListener}
 * {@link OutputGateQosReportingListener} and adds them to the respective
 * input/output gates.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosReportingListenerHelper {

	public static void listenToVertexStatisticsOnInputGate(QosReportingReader input,
			final int inputGateIndex, final VertexStatisticsReportManager vertexStatisticsManager) {

		InputGateQosReportingListener listener = new InputGateQosReportingListener() {
			private int recordsReadFromBuffer = 0;

			@Override
			public void recordReceived(int inputChannel, TimeStampedRecord record) {
				vertexStatisticsManager.recordReceived(inputGateIndex);
				recordsReadFromBuffer++;
			}

			@Override
			public void tryingToReadRecord() {
				vertexStatisticsManager.tryingToReadRecord(inputGateIndex);
			}

			@Override
			public void inputBufferConsumed(int channelIndex, long bufferInterarrivalTimeNanos) {
				vertexStatisticsManager.inputBufferConsumed(inputGateIndex, channelIndex,
						bufferInterarrivalTimeNanos, recordsReadFromBuffer);
				recordsReadFromBuffer = 0;
			}
		};

		InputGateQosReportingListener oldListener = input.getQosCallback(inputGateIndex);
		if (oldListener != null) {
			input.setQosCallback(createChainedListener(oldListener, listener), inputGateIndex);
		} else {
			input.setQosCallback(listener, inputGateIndex);
		}
	}

	public static void listenToVertexStatisticsOnOutputGate(StreamRecordWriter<?> writer, final int outputGateIndex,
			final VertexStatisticsReportManager vertexStatisticsManager) {

		OutputGateQosReportingListener listener = new OutputGateQosReportingListener() {
			@Override
			public void outputBufferSent(int channelIndex, long currentAmountTransmitted) {
				// nothing to do
			}

			@Override
			public void recordEmitted(int outputChannel, TimeStampedRecord record) {
				vertexStatisticsManager.recordEmitted(outputGateIndex, outputChannel, record);
			}

			@Override
			public void outputBufferAllocated(int channelIndex) {
				// nothing to do
			}
		};

		OutputGateQosReportingListener oldListener = writer.getQosCallback();

		if (oldListener != null) {
			writer.setQosCallback(createChainedListener(listener, oldListener));
		} else {
			writer.setQosCallback(listener);
		}
	}

	public static void listenToChannelLatenciesOnInputGate(QosReportingReader input, int inputIndex,
			final InputGateReporterManager inputGateReporter) {

		InputGateQosReportingListener listener = new InputGateQosReportingListener() {
			@Override
			public void recordReceived(int inputChannel, TimeStampedRecord record) {
				if (record.hasTimestamp()) {
					inputGateReporter.reportLatencyIfNecessary(inputChannel, record.getTimestamp());
				}
			}

			@Override
			public void tryingToReadRecord() {
				// nothing to do
			}

			@Override
			public void inputBufferConsumed(int channelIndex, long bufferInterarrivalTimeNanos) {
				// nothing to do
			}
		};

		InputGateQosReportingListener oldListener = input.getQosCallback(inputIndex);
		if (oldListener != null) {
			input.setQosCallback(createChainedListener(listener, oldListener), inputIndex);
		} else {
			input.setQosCallback(listener, inputIndex);
		}
	}

	public static void listenToOutputChannelStatisticsOnOutputGate(StreamRecordWriter<?> writer,
			final OutputGateReporterManager outputGateReporter) {

		OutputGateQosReportingListener listener = new OutputGateQosReportingListener() {
			@Override
			public void outputBufferSent(int channelIndex, long currentAmountTransmitted) {
				outputGateReporter.outputBufferSent(channelIndex, currentAmountTransmitted);
			}

			@Override
			public void recordEmitted(int outputChannel, TimeStampedRecord record) {
				outputGateReporter.recordEmitted(outputChannel, record);
			}

			@Override
			public void outputBufferAllocated(int channelIndex) {
				outputGateReporter.outputBufferAllocated(channelIndex);
			}
		};

		OutputGateQosReportingListener oldListener = writer.getQosCallback();

		if (oldListener != null) {
			writer.setQosCallback(createChainedListener(oldListener, listener));
		} else {
			writer.setQosCallback(listener);
		}
	}

	private static InputGateQosReportingListener createChainedListener(final InputGateQosReportingListener first,
			final InputGateQosReportingListener second) {

		return new InputGateQosReportingListener() {
			@Override
			public void recordReceived(int inputChannel, TimeStampedRecord record) {
				first.recordReceived(inputChannel, record);
				second.recordReceived(inputChannel, record);
			}

			@Override
			public void tryingToReadRecord() {
				first.tryingToReadRecord();
				second.tryingToReadRecord();
			}

			@Override
			public void inputBufferConsumed(int channelIndex, long bufferInterarrivalTimeNanos) {
				first.inputBufferConsumed(channelIndex, bufferInterarrivalTimeNanos);
				second.inputBufferConsumed(channelIndex, bufferInterarrivalTimeNanos);
			}
		};
	}

	private static OutputGateQosReportingListener createChainedListener(final OutputGateQosReportingListener first,
			final OutputGateQosReportingListener second) {

		return new OutputGateQosReportingListener() {

			@Override
			public void outputBufferSent(int channelIndex, long currentAmountTransmitted) {
				first.outputBufferSent(channelIndex, currentAmountTransmitted);
				second.outputBufferSent(channelIndex, currentAmountTransmitted);
			}

			@Override
			public void recordEmitted(int outputChannel, TimeStampedRecord record) {
				first.recordEmitted(outputChannel, record);
				second.recordEmitted(outputChannel, record);
			}

			@Override
			public void outputBufferAllocated(int channelIndex) {
				first.outputBufferAllocated(channelIndex);
				second.outputBufferAllocated(channelIndex);
			}
		};
	}

}
