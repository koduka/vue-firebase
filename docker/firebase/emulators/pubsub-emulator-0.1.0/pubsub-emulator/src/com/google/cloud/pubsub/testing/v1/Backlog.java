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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.pubsub.testing.v1;

import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import io.grpc.StatusException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/** An interface for message backlogs. */
interface Backlog {
  /** Inserts <code>messages</code> into the backlog. */
  void addAll(List<PubsubMessage> messages);

  /**
   * Returns at most <code>maxMessages</code> messages from unpulled messages. The default ack
   * deadline will be used.
   */
  List<ReceivedMessage> pull(int maxMessages);

  /**
   * Returns at most <code>maxMessages</code> messages from the unpulled messages. The
   * unacknowledged messages expire in <code>deadline</code>.
   */
  List<ReceivedMessage> pull(int maxMessages, Duration deadline);

  /**
   * Returns at most <code>maxMessages</code> messages as a part of a streaming pull. A stream can
   * invoke this function multiple times. For each call, it should pass its own unique <code>
   * streamId</code>. The default ack deadline will be used.
   */
  List<ReceivedMessage> streamingPull(long streamId, int maxMessages);

  /**
   * Returns at most <code>maxMessages</code> messages as a part of a streaming pull. A stream can
   * invoke this function multiple times. For each call, it should pass its own unique <code>
   * streamId</code>. Unacknowledged messages expire in <code>deadline</code>.
   */
  List<ReceivedMessage> streamingPull(long streamId, int maxMessages, Duration deadline);

  /** Called when a streaming pull is completed. */
  void onStreamingPullCompleted(long streamId);

  /** Creates and returns a snapshot of this backlog, capturing its ack set. */
  SnapshotData createSnapshot(String name) throws StatusException;

  /**
   * Performs a seek-to-snapshot on this backlog to <code>snapshot</code>. Assumes <code>snapshot
   * </code> has been pre-validated as a snapshot of the same topic.
   */
  void seek(SnapshotData snapshot) throws StatusException;

  /** Performs a seek-to-time on this backlog to <code>time</code>. */
  void seek(Instant time) throws StatusException;

  /** Sets whether acknowledged messages are being retained by this backlog. */
  void setRetainAckedMessages(boolean retainAckedMessages);

  /** Sets this backlog's default ack deadline for delivered messages. */
  void setAckDeadline(Duration ackDeadline);

  /** Returns the number of delivered but unacknowledged messages. */
  int getDeliveredCount();

  /** Acknowledges messages corresponding to <code>ackIds</code>. */
  void acknowledge(List<String> ackIds) throws StatusException;

  /** Acknowledges messages whose predecessors are all acked. */
  void cumulativeAcknowledge(List<String> ackIds) throws StatusException;

  /** Modifies the ack deadline for the unacknowledged messages specified by <code>ackIds</code>. */
  void modifyAckDeadline(List<String> ackIds, List<Long> ackDeadlinesMillis) throws StatusException;

  /** Returns true if message ordering is enabled. */
  boolean messageOrderingEnabled();
}
