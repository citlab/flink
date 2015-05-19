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

package org.apache.flink.streaming.statistics.message;

public class CpuLoadClassifier {

	public static final double MEDIUM_THRESHOLD_PERCENT = 60.0;

	public static final double HIGH_THRESHOLD_PERCENT = 85.0;

	public enum CpuLoad {
		LOW, MEDIUM, HIGH,
	}

	/**
	 * Classifies the given CPU utilization.
	 * 
	 * @param cpuUtilizationPercent
	 *            How much percent of one CPU core's available time a task's
	 *            main thread and its associated user threads consume. Example:
	 *            50 means it uses half a core's worth of CPU time. 200 means it
	 *            uses two core's worth of CPU time (this can happen if the
	 *            vertex's main thread spawns several user threads)
	 */
	public static CpuLoad fromCpuUtilization(double cpuUtilizationPercent) {
		if (cpuUtilizationPercent < MEDIUM_THRESHOLD_PERCENT) {
			return CpuLoad.LOW;
		} else if (cpuUtilizationPercent >= MEDIUM_THRESHOLD_PERCENT
				&& cpuUtilizationPercent < HIGH_THRESHOLD_PERCENT) {
			return CpuLoad.MEDIUM;
		} else {
			return CpuLoad.HIGH;
		}
	}
}
