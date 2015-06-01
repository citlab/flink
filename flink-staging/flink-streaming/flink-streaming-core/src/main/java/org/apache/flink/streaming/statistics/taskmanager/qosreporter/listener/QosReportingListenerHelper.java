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
import org.apache.flink.streaming.runtime.io.StreamingAbstractRecordReader;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.InputGateReporterManager;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.OutputGateReporterManager;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.TimestampTag;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.vertex.VertexStatisticsReportManager;
import org.apache.flink.streaming.statistics.types.AbstractTaggableRecord;

/**
 * Utility class that creates {@link InputGateQosReportingListener}
 * {@link OutputGateQosReportingListener} and adds them to the respective
 * input/output gates.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosReportingListenerHelper {

	public static void listenToVertexStatisticsOnInputGate(StreamingAbstractRecordReader<?> input,
			final int inputGateIndex, final VertexStatisticsReportManager vertexStatisticsManager) {

		InputGateQosReportingListener listener = new InputGateQosReportingListener() {
			@Override
			public void recordReceived(int inputChannel, AbstractTaggableRecord record) {
				vertexStatisticsManager.recordReceived(inputChannel);
			}

			@Override
			public void tryingToReadRecord() {
				vertexStatisticsManager.tryingToReadRecord(inputGateIndex);
			}

			@Override
			public void inputBufferConsumed(int channelIndex, long bufferInterarrivalTimeNanos, int recordsReadFromBuffer) {
				vertexStatisticsManager.inputBufferConsumed(inputGateIndex, channelIndex, bufferInterarrivalTimeNanos,
						recordsReadFromBuffer);
			}
		};

		InputGateQosReportingListener oldListener = input.getQosCallback();
		if (oldListener != null) {
			input.setQosCallback(createChainedListener(oldListener, listener));
		} else {
			input.setQosCallback(listener);
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
			public void recordEmitted(int outputChannel, AbstractTaggableRecord record) {
				vertexStatisticsManager.recordEmitted(outputGateIndex);
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

	public static void listenToChannelLatenciesOnInputGate(StreamingAbstractRecordReader<?> input,
			final InputGateReporterManager inputGateReporter) {

		InputGateQosReportingListener listener = new InputGateQosReportingListener() {
			@Override
			public void recordReceived(int inputChannel, AbstractTaggableRecord record) {
				TimestampTag timestampTag = (TimestampTag) record.getTag();

				if (timestampTag != null) {
					inputGateReporter.reportLatencyIfNecessary(inputChannel, timestampTag);
				}
			}

			@Override
			public void tryingToReadRecord() {
				// nothing to do
			}

			@Override
			public void inputBufferConsumed(int channelIndex, long bufferInterarrivalTimeNanos, int recordsReadFromBuffer) {
				// nothing to do
			}
		};

		InputGateQosReportingListener oldListener = input.getQosCallback();
		if (oldListener != null) {
			input.setQosCallback(createChainedListener(listener, oldListener));
		} else {
			input.setQosCallback(listener);
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
			public void recordEmitted(int outputChannel, AbstractTaggableRecord record) {
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
			public void recordReceived(int inputChannel, AbstractTaggableRecord record) {
				first.recordReceived(inputChannel, record);
				second.recordReceived(inputChannel, record);
			}

			@Override
			public void tryingToReadRecord() {
				first.tryingToReadRecord();
				second.tryingToReadRecord();
			}

			@Override
			public void inputBufferConsumed(int channelIndex, long bufferInterarrivalTimeNanos, int recordsReadFromBuffer) {
				
				first.inputBufferConsumed(channelIndex, bufferInterarrivalTimeNanos, recordsReadFromBuffer);
				second.inputBufferConsumed(channelIndex, bufferInterarrivalTimeNanos, recordsReadFromBuffer);
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
			public void recordEmitted(int outputChannel, AbstractTaggableRecord record) {
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
