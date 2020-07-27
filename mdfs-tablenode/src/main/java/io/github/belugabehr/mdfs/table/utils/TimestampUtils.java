package io.github.belugabehr.mdfs.table.utils;

import java.time.Instant;

import com.google.protobuf.Timestamp;

public class TimestampUtils {

	private TimestampUtils() {

	}

	public static Timestamp now() {
		final Instant now = Instant.now();
		return Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
	}

}
