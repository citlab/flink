package org.apache.flink.streaming.statistics.taskmanager.qosmanager;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.streaming.statistics.JobGraphLatencyConstraint;
import org.apache.flink.streaming.statistics.LatencyConstraintID;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosGraph;
import org.apache.flink.streaming.statistics.util.QosStatisticsConfig;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QosWebStatistic {
	private static final Logger LOG = LoggerFactory.getLogger(QosWebStatistic.class);

	private final String jobName;

	private final long jobCreationTimestamp;

	private final long loggingInterval;

	private final Map<LatencyConstraintID, JobGraphLatencyConstraint> qosConstraints;

	private final HashMap<LatencyConstraintID, QosInMemoryLogger> qosMemoryLogger = new HashMap<LatencyConstraintID, QosInMemoryLogger>();


	public QosWebStatistic(ExecutionGraph execGraph, QosGraph qosGraph) {
		this.jobName = execGraph.getJobName();
		this.jobCreationTimestamp = System.currentTimeMillis();
		this.qosConstraints = qosGraph.getConstraintsWithId();
		this.loggingInterval = QosStatisticsConfig.getAdjustmentIntervalMillis();

		for(JobGraphLatencyConstraint constraint : qosConstraints.values()) {
			qosMemoryLogger.put(constraint.getID(), new QosInMemoryLogger(execGraph, constraint, loggingInterval));
		}
	}

	public long getRefreshInterval() {
		return this.loggingInterval;
	}

	public int getMaxEntriesCount() {
		return qosMemoryLogger.values().iterator().next().getMaxEntriesCount();
	}

	public void logConstraintSummaries(List<QosConstraintSummary> constraintSummaries) {
		for (QosConstraintSummary constraintSummary : constraintSummaries) {
			LatencyConstraintID constraintId = constraintSummary.getLatencyConstraintID();

			try {
				if (this.qosMemoryLogger.containsKey(constraintId))
					this.qosMemoryLogger.get(constraintId).logSummary(constraintSummary);

			} catch (Exception e) {
				LOG.error("Error during QoS logging", e);
			}
		}
	}

	public JSONObject getStatistics(JSONObject jobJson, long startTimestamp) throws JSONException {
		JSONObject constraints = new JSONObject();

		for(LatencyConstraintID id : this.qosConstraints.keySet()) {
			JSONObject constraint = new JSONObject();
			constraint.put("name", this.qosConstraints.get(id).getName());

			if (startTimestamp > 0) {
				this.qosMemoryLogger.get(id).toJson(constraint, startTimestamp);
			} else {
				this.qosMemoryLogger.get(id).toJson(constraint);
			}

			constraints.put(id.toString(), constraint);
		}

		jobJson.put("constraints", constraints);
		return jobJson;
	}

	public String doGet(HttpServletRequest req) {
		try {
			long startTimestamp = -1;

			JSONObject result = new JSONObject();
			result.put("name", this.jobName);
			result.put("creationTimestamp", this.jobCreationTimestamp);
			result.put("creationFormatted", new Date(this.jobCreationTimestamp));
			result.put("refreshInterval", getRefreshInterval());
			result.put("maxEntriesCount", getMaxEntriesCount());

			if (req != null && req.getParameter("startTimestamp") != null && !req.getParameter("startTimestamp").isEmpty()) {
				startTimestamp = Long.parseLong(req.getParameter("startTimestamp"));
			}
			getStatistics(result, startTimestamp);

			return result.toString();

		} catch (JSONException e) {
			LOG.error("JSON Error: " + e.getMessage(), e);
			return "{ status: \"internal error\", message: \"" + e.getMessage() + "\" }";
		}
	}
}
