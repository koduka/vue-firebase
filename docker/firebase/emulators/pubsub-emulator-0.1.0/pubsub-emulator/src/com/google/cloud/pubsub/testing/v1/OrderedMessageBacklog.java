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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Subscription;
import io.grpc.Status;
import io.grpc.StatusException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A message backlog that supports ordering keys. The messages that contain the same ordering key
 * are stored in the same MessageBacklog object, so they can be delivered in order. The total order
 * across ordering keys is not guaranteed. Thread-safe.
 */
class OrderedMessageBacklog implements Backlog {
  // This maximum may not reflect the maximum enforced by the actual service.
  private static final int MAX_MESSAGES_PER_PULL_REQUEST = 100;
  // The maximum number of messages pulled from a single backlog per each iteration.
  private static final int MAX_MESSAGES_PER_SINGLE_BACKLOG_PULL = 3;

  private final String subscriptionName;
  private final String topicName;
  private Duration ackDeadline;

  private final AtomicLong ackIdProvider;
  private final Clock clock;
  private final OrderingKeyHasher orderingKeyHasher;
  // Maps ordering keys to message backlogs.
  private final LinkedHashMap<String, MessageBacklog> backlogs;
  // Maps ordering keys to stream IDs.
  private final HashMap<String, Long> orderingKeysToStreamIds;
  // Maps stream IDs to the current indexes of the streams. The indexes are always between 0 and n -
  // 1, where n is the current number of streams.
  private final HashMap<Long, Integer> streamIdsToIndexes;
  private final Random rand;

  // Whether acked messages are being retained by this backlog.
  private boolean retainAckedMessages;

  OrderedMessageBacklog(Subscription subscription, AtomicLong ackIdProvider) {
    this(subscription, ackIdProvider, new RealClock(), new RealOrderingKeyHasher());
  }

  OrderedMessageBacklog(
      Subscription subscription,
      AtomicLong ackIdProvider,
      Clock clock,
      OrderingKeyHasher orderingKeyHasher) {

    this(
        subscription.getName(),
        subscription.getTopic(),
        Duration.ofSeconds(subscription.getAckDeadlineSeconds()),
        ackIdProvider,
        clock,
        orderingKeyHasher);

    if (!subscription.getEnableMessageOrdering()) {
      throw new IllegalStateException(
          "Attempted to create an OrderedMessageBacklog from a subscription without message"
              + " ordering enabled.");
    }
  }

  OrderedMessageBacklog(
      String subscriptionName,
      String topicName,
      Duration ackDeadline,
      AtomicLong ackIdProvider,
      Clock clock,
      OrderingKeyHasher orderingKeyHasher) {
    this.subscriptionName = subscriptionName;
    this.topicName = topicName;
    this.ackDeadline = ackDeadline;
    this.ackIdProvider = ackIdProvider;
    this.clock = clock;
    this.orderingKeyHasher = orderingKeyHasher;
    this.backlogs = new LinkedHashMap<>();
    this.orderingKeysToStreamIds = new HashMap<>();
    this.streamIdsToIndexes = new HashMap<>();
    this.rand = new Random();
  }

  /**
   * Finds (or creates if not exist) a message backlog mapped to the ordering key of each message,
   * then adds the message to the backlog. If the ordering key is empty, add the message into the
   * backlog associated with the empty string.
   */
  @Override
  public synchronized void addAll(List<PubsubMessage> messages) {
    for (PubsubMessage message : messages) {
      MessageBacklog backlog = backlogs.get(message.getOrderingKey());
      if (backlog == null) {
        backlog =
            new MessageBacklog(
                subscriptionName,
                topicName,
                ackDeadline,
                ackIdProvider,
                MessageBacklog.RedeliveryPolicy.RESEND_ALL,
                clock);
        backlog.setRetainAckedMessages(retainAckedMessages);
        backlogs.put(message.getOrderingKey(), backlog);
      }
      backlog.addAll(ImmutableList.of(message));
    }
  }

  /**
   * Pulls at most <code>maxMessages</code> messages from the underlying backlogs. The default ack
   * deadline specified in the constructor will be used.
   */
  @Override
  public List<ReceivedMessage> pull(int maxMessages) {
    return pull(maxMessages, ackDeadline);
  }

  /**
   * Pulls at most <code>maxMessages</code> messages from the underlying backlogs. If
   * `order_messages` in the subscription is true, the messages with the same ordering key will be
   * pulled in the order in which they are published. In this case, the messages are pulled from
   * backlogs in a round-robin manner. Only one message is pulled from a backlog, then the next
   * message will be pulled from the next backlog.
   */
  @Override
  public synchronized List<ReceivedMessage> pull(int maxMessages, Duration deadline) {
    return pullFromBacklogs(getAllBacklogsForPull(), maxMessages, deadline);
  }

  // Returns backlogs for a regular pull.
  private ImmutableMap<String, MessageBacklog> getAllBacklogsForPull() {

    // If there is no streaming pull, the regular pull is allowed to fetch messages from any
    // backlogs regardless of the ordering keys. Therefore, return all the backlogs.
    if (streamIdsToIndexes.isEmpty()) {
      return ImmutableMap.copyOf(backlogs);
    }
    // Otherwise, return the backlog for the messages with no ordering key.
    return ImmutableMap.of("", backlogs.get(""));
  }

  /**
   * Pulls messages as a part of a streaming pull. A stream can invoke this function multiple times.
   * For each call, it should pass its own unique stream id to receive related messages, which have
   * the same ordering key, consistently. The default ack deadline specified in the constuctor will
   * be used.
   */
  @Override
  public List<ReceivedMessage> streamingPull(long streamId, int maxMessages) {
    return streamingPull(streamId, maxMessages, ackDeadline);
  }

  /**
   * Pulls messages as a part of a streaming pull. A stream can invoke this function multiple times.
   * For each call, it should pass its own unique stream id to receive related messages, which have
   * the same ordering key, consistently.
   */
  @Override
  public synchronized List<ReceivedMessage> streamingPull(
      long streamId, int maxMessages, Duration deadline) {
    // If the stream is new, store stream and set its index.
    if (!streamIdsToIndexes.containsKey(streamId)) {
      streamIdsToIndexes.put(streamId, streamIdsToIndexes.size());
    }
    // Pull messages from all the backlogs assigned to this stream.
    return pullFromBacklogs(getAllBacklogsForStreamingPull(streamId), maxMessages, deadline);
  }

  // Returns all the backlogs assigned to the given stream.
  private ImmutableMap<String, MessageBacklog> getAllBacklogsForStreamingPull(long streamId) {
    Integer streamIndex = streamIdsToIndexes.get(streamId);
    if (streamIndex == null) {
      return ImmutableMap.of();
    }
    ImmutableMap.Builder<String, MessageBacklog> backlogsBuilder = ImmutableMap.builder();
    for (Map.Entry<String, MessageBacklog> backlogEntry : backlogs.entrySet()) {
      String orderingKey = backlogEntry.getKey();
      MessageBacklog backlog = backlogEntry.getValue();
      // Always add the backlog for the non-ordered messages since those messages can be pulled by
      // any pull requests.
      if (orderingKey.isEmpty()) {
        backlogsBuilder.put("", backlog);
        continue;
      }
      // Skip the backlog if it is not assigned to this stream.
      int orderingKeyShardIndex =
          orderingKeyHasher.hashCode(orderingKey, streamIdsToIndexes.size());
      if (streamIndex != orderingKeyShardIndex) {
        continue;
      }
      // Skip the backlog if another stream is already receiving messages with the ordering key and
      // some of them have not been acked yet.
      Long existingStream = orderingKeysToStreamIds.get(orderingKey);
      if (existingStream != null && existingStream != streamId && backlog.getDeliveredCount() > 0) {
        continue;
      }
      // Otherwise, assign the ordering key to the stream, and add the backlog to the list to be
      // returned.
      orderingKeysToStreamIds.put(orderingKey, streamId);
      backlogsBuilder.put(orderingKey, backlog);
    }
    return backlogsBuilder.build();
  }

  /**
   * Called when a streaming pull is completed. The indexes of existing streams will be
   * recalculated, which causes reassignment of ordering keys to the streams.
   */
  @Override
  public synchronized void onStreamingPullCompleted(long streamId) {
    // Remove the stream index mapped to `streamId`.
    streamIdsToIndexes.remove(streamId);
    // Remove the stream ID mapped to an ordering key.
    Iterator<Map.Entry<String, Long>> it = orderingKeysToStreamIds.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, Long> entry = it.next();
      if (entry.getValue().equals(streamId)) {
        it.remove();
      }
    }
    recalculateStreamIndex();
  }

  private void recalculateStreamIndex() {
    int index = 0;
    // Reset the index of each stream. If there are empty indexes due to terminated streams, this
    // will shift existing streams left to fill up the empty index(es).
    for (Map.Entry<Long, Integer> element : streamIdsToIndexes.entrySet()) {
      element.setValue(index++);
    }
  }

  /**
   * Pulls at most <code>maxMessages</code> messages from the underlying backlogs. The messages are
   * pulled from backlogs in a round-robin manner. Only one message is pulled from a backlog, then
   * the next message will be pulled from the next backlog. The messages with the same ordering key
   * will be pulled in the order in which they are published.
   */
  private synchronized List<ReceivedMessage> pullFromBacklogs(
      Map<String, MessageBacklog> backlogsForPull, int maxMessages, Duration deadline) {
    List<ReceivedMessage> results = new ArrayList<>();
    maxMessages = Math.min(maxMessages, MAX_MESSAGES_PER_PULL_REQUEST);
    int maxMessagesSingleBacklog = Math.min(maxMessages, MAX_MESSAGES_PER_SINGLE_BACKLOG_PULL);
    while (results.size() < maxMessages) {
      int resultSizeBeforeIteration = results.size();
      // By pulling messages in a round-robin manner from each backlog, we intentionally change the
      // total order across ordering keys to simulate the Pub/Sub system which does not guarantee
      // the total order.
      for (Map.Entry<String, MessageBacklog> entry : backlogsForPull.entrySet()) {
        MessageBacklog backlog = entry.getValue();
        int numToRead = Math.min(maxMessagesSingleBacklog, maxMessages - results.size());
        List<ReceivedMessage> messages = backlog.pull(rand.nextInt(numToRead) + 1, deadline);
        results.addAll(messages);
        if (results.size() >= maxMessages) {
          break;
        }
      }
      // Nothing added. All the backlogs are empty.
      if (results.size() == resultSizeBeforeIteration) {
        break;
      }
    }
    return ImmutableList.copyOf(results);
  }

  /**
   * Returns the number of current underlying backlogs. This number is identical to the number of
   * ordering keys since a backlog is created per ordering key. This function can be used for
   * logging and testing. (This function is not a part of Backlog interface.)
   */
  @VisibleForTesting
  synchronized int getBacklogsCount() {
    return backlogs.size();
  }

  @Override
  public synchronized int getDeliveredCount() {
    int sum = 0;
    for (MessageBacklog backlog : backlogs.values()) {
      sum += backlog.getDeliveredCount();
    }
    return sum;
  }

  /**
   * Acknowleges messages specified by {@code ackIds}. {@code OrderedMessageBacklogs} acknowledges
   * the messages cumulatively by default.
   */
  @Override
  public synchronized void acknowledge(List<String> ackIds) throws StatusException {
    cumulativeAcknowledge(ackIds);
  }

  /** Acknowledges messages whose predecessors are all acked. */
  @Override
  public synchronized void cumulativeAcknowledge(List<String> ackIds) throws StatusException {
    for (MessageBacklog backlog : backlogs.values()) {
      backlog.cumulativeAcknowledge(ackIds);
    }
  }

  /** Modifies the ack deadline for the unacknowledged messages specified by <code>ackIds</code>. */
  @Override
  public synchronized void modifyAckDeadline(List<String> ackIds, List<Long> ackDeadlinesMillis)
      throws StatusException {
    for (Map.Entry<String, MessageBacklog> backlogEntry : backlogs.entrySet()) {
      backlogEntry.getValue().modifyAckDeadline(ackIds, ackDeadlinesMillis);
    }
  }

  /**
   * Creates and returns a snapshot of this backlog. The ack set of an ordered subscription is the
   * union of all the ack sets of its individual backlogs, as each backlog contains all messages
   * from one specific ordering key.
   */
  @Override
  public synchronized SnapshotData createSnapshot(String name) throws StatusException {
    Set<String> union = new HashSet<>();

    for (MessageBacklog backlog : backlogs.values()) {
      union.addAll(backlog.getAckSet());
    }

    return new SnapshotData(
        name, topicName, /*isOrdered=*/ true, union, oldestUnackedPublishTime(), clock);
  }

  @Override
  public synchronized void seek(SnapshotData snapshot) throws StatusException {
    if (!snapshot.isOrdered()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "Subscriptions with ordering keys enabled cannot be seeked to snapshots of"
                  + " subscriptions without ordering keys enabled, as it may cause"
                  + " out-of-order redelivery.")
          .asException();
    }
    for (MessageBacklog backlog : backlogs.values()) {
      // Seek each sub-backlog to the snapshot. If a message ID in the snapshot's ack set is not
      // present in a sub-backlog (because the message actually belongs to a sub-backlog for a
      // different ordering key), it will simply be ignored.
      backlog.seek(snapshot);
    }
  }

  @Override
  public synchronized void seek(Instant time) throws StatusException {
    // TODO(b/137395538): Seeking to time on ordered subscriptions has unresolved complications with
    // message ordering and message retention that may cause out-of-order message delivery.
    throw Status.UNIMPLEMENTED.withDescription("Unimplemented").asException();
  }

  @Override
  public synchronized void setRetainAckedMessages(boolean retainAckedMessages) {
    this.retainAckedMessages = retainAckedMessages;
    for (MessageBacklog backlog : backlogs.values()) {
      backlog.setRetainAckedMessages(retainAckedMessages);
    }
  }

  @Override
  public synchronized void setAckDeadline(Duration ackDeadline) {
    this.ackDeadline = ackDeadline;
    for (MessageBacklog backlog : backlogs.values()) {
      backlog.setAckDeadline(ackDeadline);
    }
  }

  /**
   * Returns the publish time of the oldest unacked message across all backlogs. If there are no
   * unacked messages, returns Instant.MAX.
   */
  Instant oldestUnackedPublishTime() {
    Instant oldestUnackedPublishTime = Instant.MAX;

    for (MessageBacklog backlog : backlogs.values()) {
      Instant oldestTimeForThisKey = backlog.oldestUnackedPublishTime();
      if (oldestTimeForThisKey.isBefore(oldestUnackedPublishTime)) {
        oldestUnackedPublishTime = oldestTimeForThisKey;
      }
    }

    return oldestUnackedPublishTime;
  }

  @Override
  public boolean messageOrderingEnabled() {
    // Return true because `ordering_key` is enabled in this class.
    return true;
  }

  private static class RealOrderingKeyHasher implements OrderingKeyHasher {
    @Override
    public int hashCode(String key, int numBuckets) {
      return Hashing.consistentHash(key.hashCode(), numBuckets);
    }
  }
}
