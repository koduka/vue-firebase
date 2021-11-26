// Copyright 2019 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.pubsub.testing.v1;

import com.google.common.collect.ImmutableSet;
import com.google.pubsub.v1.Snapshot;
import io.grpc.Status;
import io.grpc.StatusException;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

/**
 * A snapshot, containing a frozen copy of all the acked message IDs of a subscription. Contains one
 * set of message IDs per ordering key in the snapshotted subscription, if it has ordering keys
 * enabled. Contains only one set of message IDs if the subscription does not have ordering keys
 * enabled.
 *
 * <p>The constructor throws a Status.FAILED_PRECONDITION exception if the created snapshot would
 * have already expired or would have a lifetime less than <code>MIN_LIFETIME_MILLIS</code>.
 */
class SnapshotData {
  // A snapshot expires 1 week after the publish time of its oldest unacked message.
  static final long RETENTION_TIME_MILLIS = 60 * 60 * 24 * 7 * 1000;
  // A snapshot must have a minimum lifetime of 1 hour.
  static final long MIN_LIFETIME_MILLIS = 60 * 60 * 1000;

  Snapshot snapshot;
  ImmutableSet<String> ackSet;
  boolean isOrdered;
  Clock clock;

  SnapshotData(
      String name,
      String topic,
      boolean isOrdered,
      Set<String> ackSet,
      Instant oldestUnackedPublishTime,
      Clock clock)
      throws StatusException {
    this.clock = clock;
    Instant expireTime = calculateExpireTime(clock, oldestUnackedPublishTime);
    this.snapshot =
        Snapshot.newBuilder()
            .setName(name)
            .setTopic(topic)
            .setExpireTime(JavaTimeConversions.toProtoTimestamp(expireTime))
            .build();
    this.isOrdered = isOrdered;
    this.ackSet = ImmutableSet.copyOf(ackSet);
  }

  public boolean isOrdered() {
    return isOrdered;
  }

  public Snapshot getConfig() {
    return snapshot;
  }

  public boolean isExpired() {
    Instant now = Instant.ofEpochMilli(clock.currentTimeMillis());
    return !now.isBefore(JavaTimeConversions.toJavaInstant(snapshot.getExpireTime()));
  }

  public ImmutableSet<String> getAckSet() {
    return ackSet;
  }

  /**
   * Calculates and returns the expiration time of a snapshot of a subscription whose oldest unacked
   * message was published at <code>oldestUnackedPublishTime</code>. Throws
   * Status.FAILED_PRECONDITION if the snapshot would have an invalid lifetime.
   */
  static Instant calculateExpireTime(Clock clock, Instant oldestUnackedPublishTime)
      throws StatusException {
    // TODO(b/136094663): This implementation ignores the lack of 7-day retention, so snapshots
    // cannot be created on subscriptions with unacked messages older than the max snapshot
    // lifetime.
    Instant now = Instant.ofEpochMilli(clock.currentTimeMillis());
    if (oldestUnackedPublishTime.equals(Instant.MAX)) {
      // A snapshot with no unacked messages expires in a week.
      return now.plusMillis(RETENTION_TIME_MILLIS);
    }

    Instant expireTime = oldestUnackedPublishTime.plusMillis(RETENTION_TIME_MILLIS);
    Duration lifetime = Duration.between(now, expireTime);
    if (lifetime.compareTo(Duration.ofMillis(MIN_LIFETIME_MILLIS)) < 0) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "The operation could not be completed because the requested source subscription's"
                  + " backlog is too old.")
          .asException();
    }
    return expireTime;
  }
}
