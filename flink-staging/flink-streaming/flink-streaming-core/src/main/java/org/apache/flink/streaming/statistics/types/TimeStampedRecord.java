package org.apache.flink.streaming.statistics.types;

public interface TimeStampedRecord {

	boolean hasTimestamp();

	long getTimestamp();

	void setTimestamp(long timestamp);
}
