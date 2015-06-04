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

package org.apache.flink.streaming.statistics.util;

import org.apache.flink.configuration.GlobalConfiguration;

public class QosStatisticsConfig {

	/**
	 * Name of configuration entry which defines the interval in which received
	 * tags shall be aggregated and sent to the job manager plugin component.
	 */
	public static final String AGGREGATION_INTERVAL_KEY = "streaming.qosreporter.aggregationinterval";

	/**
	 * The default aggregation interval.
	 */
	public static final long DEFAULT_AGGREGATION_INTERVAL = 1000;

	/**
	 * Name of the configuration entry which defines the interval in which
	 * records shall be tagged.
	 */
	public static final String SAMPLING_PROBABILITY_KEY = "streaming.qosreporter.samplingprobability";

	/**
	 * The default sampling probability in percent.
	 */
	public static final int DEFAULT_SAMPLING_PROBABILITY = 10;

	/**
	 * Name of the configuration entry which defines the QoS statistics log file
	 * location.
	 */
	public static final String QOS_STAT_LOGFILE_PATTERN_KEY = "streaming.qosmanager.logging.qos_statistics_filepattern";

	public static final String DEFAULT_QOS_STAT_LOGFILE_PATTERN = "/tmp/qos_statistics_%s";

	/**
	 * Name of the configuration entry which defines the CPU statistics log file
	 * location.
	 */
	private static final String CPU_STAT_LOGFILE_PATTERN_KEY = "streaming.qosmanager.logging.cpu_statistics_filepattern";

	private static final String DEFAULT_CPU_STAT_LOGFILE_PATTERN = "/tmp/cpu_statistics_%s";

	/**
	 * Name of the configuration entry which defines the time interval for QoS
	 * driven adjustments.
	 */
	public static final String QOSMANAGER_ADJUSTMENTINTERVAL_KEY = "streaming.qosmanager.adjustmentinterval";

	public static final long DEFAULT_ADJUSTMENTINTERVAL = 5000;

	/**
	 * Poolsize of thread pool used for flushing output channels. It is better to err on the
	 * high side here, because setting this too low causes buffers to not get flushed in
	 * time for their deadline.
	 */
	public static final String OUTPUT_CAHNNEL_FLUSHER_THREADPOOLSIZE_KEY = "streaming.runtime.output_channel_flusher_threadpoolsize";

	public static final int DEFAULT_OUTPUT_CAHNNEL_FLUSHER_THREADPOOLSIZE = 20;

	/**
	 * Keep history of last 15min by default: 15 60 /
	 * (DEFAULT_ADJUSTMENTINTERVAL / 1000)) = 180
	 */
	public static final String IN_MEMORY_LOG_ENTRIES_KEY = "streaming.qosmanager.logging.in_memory_entries";

	public static final int DEFAULT_IN_MEMORY_LOG_ENTRIES = 180;

	public static long getAggregationIntervalMillis() {
		return GlobalConfiguration.getLong(AGGREGATION_INTERVAL_KEY,
				DEFAULT_AGGREGATION_INTERVAL);
	}

	public static long getAdjustmentIntervalMillis() {
		return GlobalConfiguration.getLong(QOSMANAGER_ADJUSTMENTINTERVAL_KEY,
				DEFAULT_ADJUSTMENTINTERVAL);
	}

	public static int getSamplingProbabilityPercent() {
		return GlobalConfiguration.getInteger(SAMPLING_PROBABILITY_KEY,
				DEFAULT_SAMPLING_PROBABILITY);
	}

	public static int getNoOfInMemoryLogEntries() {
		return GlobalConfiguration.getInteger(IN_MEMORY_LOG_ENTRIES_KEY,
				DEFAULT_IN_MEMORY_LOG_ENTRIES);
	}

	public static String getQosStatisticsLogfilePattern() {
		return GlobalConfiguration.getString(QOS_STAT_LOGFILE_PATTERN_KEY,
				DEFAULT_QOS_STAT_LOGFILE_PATTERN);
	}

	public static String getCpuStatisticsLogfilePattern() {
		return GlobalConfiguration.getString(CPU_STAT_LOGFILE_PATTERN_KEY,
				DEFAULT_CPU_STAT_LOGFILE_PATTERN);
	}

	public static int computeQosStatisticWindowSize() {
		return (int) Math.ceil(((double) getAdjustmentIntervalMillis())
				/ getAggregationIntervalMillis());
	}

	public static int getOutputChannelFlusherThreadpoolsize() {
		return GlobalConfiguration.getInteger(OUTPUT_CAHNNEL_FLUSHER_THREADPOOLSIZE_KEY,
						DEFAULT_OUTPUT_CAHNNEL_FLUSHER_THREADPOOLSIZE);
	}

}
