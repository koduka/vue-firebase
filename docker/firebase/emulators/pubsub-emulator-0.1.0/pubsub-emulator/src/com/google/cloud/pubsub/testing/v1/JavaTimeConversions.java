package com.google.cloud.pubsub.testing.v1;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.time.Instant;

/**
 * A simplified version of com.google.protobuf.util.JavaTimeConversions. The copied version is not
 * open sourced.
 */
final class JavaTimeConversions {

  /** Converts a protobuf {@link Timestamp} to a {@link java.time.Instant}. */
  public static Instant toJavaInstant(Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }

  /**
   * Converts a {@link java.time.Instant} to a protobuf {@link Timestamp}.
   *
   * @throws IllegalArgumentException if the given {@link java.time.Instant} cannot legally fit into
   *     a {@link Timestamp}. See {@link Timestamps#isValid}.
   */
  public static Timestamp toProtoTimestamp(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }
}
