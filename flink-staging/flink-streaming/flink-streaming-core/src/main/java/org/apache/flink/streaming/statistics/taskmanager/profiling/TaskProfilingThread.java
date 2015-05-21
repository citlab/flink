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

package org.apache.flink.streaming.statistics.taskmanager.profiling;

import akka.actor.ActorRef;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.profiling.ProfilingException;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.streaming.statistics.message.CpuLoadClassifier;
import org.apache.flink.streaming.statistics.message.TaskCpuLoadChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread that collects CPU profiling data on registered tasks.
 * 
 * This class is thread-safe.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class TaskProfilingThread extends Thread {

	private final static Logger LOG = LoggerFactory.getLogger(TaskProfilingThread.class);

	private static final ThreadMXBean tmx = ManagementFactory.getThreadMXBean();

	private static TaskProfilingThread singletonInstance = null;

	private static final ConcurrentHashMap<ExecutionAttemptID, TaskInfo> tasks = new ConcurrentHashMap<ExecutionAttemptID, TaskInfo>();

	private final HashMap<ExecutionAttemptID, Double> taskCpuUtilizations = new HashMap<ExecutionAttemptID, Double>();

	private final ActorRef jobManager;

	private TaskProfilingThread(ActorRef jobManager) throws ProfilingException {
		this.jobManager = jobManager;

		// Initialize MX interface and check if thread contention monitoring is
		// supported
		if (tmx.isThreadContentionMonitoringSupported()) {
			tmx.setThreadContentionMonitoringEnabled(true);
		} else {
			throw new ProfilingException("The thread contention monitoring is not supported.");
		}

		this.setName("TaskProfiler");
	}

	@Override
	public void run() {
		LOG.info("TaskProfiler thread started");
		try {
			while (!interrupted()) {
				this.collectThreadProfilingData();
				this.notifyJobManagerOfLoadStateChanges();
				Thread.sleep(1000);
			}
		} catch (InterruptedException e) {

		} finally {
			cleanUp();
			LOG.info("TaskProfiler thread stopped.");
		}
	}

	private void notifyJobManagerOfLoadStateChanges()
			throws InterruptedException {
		
		for (TaskInfo taskInfo : tasks.values()) {
			Double lastNotified = taskCpuUtilizations.get(taskInfo.getExecutionId());
			Double curr = getCpuUtilization(taskInfo);

			boolean mustNotify = (lastNotified == null && curr != null)
					|| (lastNotified != null 
					     && curr != null 
					     && ((Math.abs(lastNotified - curr) > 5) 
					    		 || CpuLoadClassifier.fromCpuUtilization(lastNotified) !=
					    		    CpuLoadClassifier.fromCpuUtilization(curr)));

			if (mustNotify) {
				taskCpuUtilizations.put(taskInfo.getExecutionId(), curr);
				jobManager.tell(
					new TaskCpuLoadChange(taskInfo.getExecutionId(), taskInfo.getCPUUtilization()),
					ActorRef.noSender()
				);
			}
		}

	}

	private Double getCpuUtilization(TaskInfo taskInfo) {
		if(!taskInfo.hasCPUUtilizationMeasurements()) {
			return  null;
		}
		
		return taskInfo.getCPUUtilization();
	}

	private void cleanUp() {
		// FIXME clean up data structures here
	}

	private void collectThreadProfilingData() {
		for (TaskInfo taskInfo : tasks.values()) {
			taskInfo.measureCpuUtilization();
		}
	}

	public static synchronized void shutdown() {
		if (singletonInstance != null) {
			singletonInstance.interrupt();
			singletonInstance = null;
		}
	}

	private static void ensureProfilingThreadIsRunning(ActorRef jobManager) throws ProfilingException {
		if (singletonInstance == null) {
			singletonInstance = new TaskProfilingThread(jobManager);
			singletonInstance.start();
		}
	}

	public static synchronized void registerTask(Task task)
			throws ProfilingException, IOException {

		TaskInfo taskInfo = new TaskInfo(task, tmx);
		tasks.put(task.getExecutionId(), taskInfo);

		ensureProfilingThreadIsRunning(task.getEnvironment().getJobManager()); // TODO: dont do this
	}

	public static synchronized void unregisterTask(ExecutionAttemptID executionID) {
		TaskInfo taskInfo = tasks.remove(executionID);
		if (taskInfo != null) {
			taskInfo.cleanUp();
		}

		if (tasks.isEmpty()) {
			shutdown();
		}
	}

	public static TaskInfo getTaskInfo(ExecutionAttemptID vertexId) {
		return tasks.get(vertexId);
	}
}
