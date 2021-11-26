// Copyright 2016 Google Inc. All Rights Reserved.
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

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Subscription;
import io.grpc.Status;
import io.grpc.StatusException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/** A backlog of messages that have not been completely delivered to a subscriber. Thread-safe. */
class MessageBacklog implements Backlog {
  // This maximum may not reflect the maximum enforced by the actual service.
  private static final int MAX_MESSAGES_PER_PULL_REQUEST = 100;

  private final String subscriptionName;
  private final String topicName;
  private Duration ackDeadline;

  private final AtomicLong ackIdProvider;
  private final Clock clock;

  // All messages published to the topic, ordered by publish time. Maps message IDs to messages.
  // Includes acknowledged and unacknowledged messages.
  private final LinkedHashMap<String, PubsubMessage> messages;

  // Message IDs of acknowledged messages.
  private Set<String> ackSet;

  // Message IDs of unacknowledged messages that are undelivered, ordered by publish time.
  private LinkedHashSet<String> undelivered;

  // Unacknowledged messages that have been delivered to a subscriber. Maps ack IDs to their
  // delivery status.
  private final Map<String, DeliveryInfo> delivered;

  // Whether acked messages are being retained by this backlog.
  private boolean retainAckedMessages;

  // Acked message IDs that have been retained according to `retain_acked_messages`.
  private final Set<String> retained;

  private final RedeliveryPolicy redeliveryPolicy;

  /** Determines which messages should be re-delivered when ack deadline for a message is expired */
  public enum RedeliveryPolicy {
    RESEND_EXPIRED, // Redeliver the expired message only.
    RESEND_ALL // Redeliver the expired message as well as its successors.
  }

  MessageBacklog(
      Subscription subscription, AtomicLong ackIdProvider, RedeliveryPolicy redeliveryPolicy) {
    this(subscription, ackIdProvider, redeliveryPolicy, new RealClock());
  }

  MessageBacklog(
      Subscription subscription,
      AtomicLong ackIdProvider,
      RedeliveryPolicy redeliveryPolicy,
      Clock clock) {
    this(
        subscription.getName(),
        subscription.getTopic(),
        Duration.ofSeconds(subscription.getAckDeadlineSeconds()),
        ackIdProvider,
        redeliveryPolicy,
        clock);

    if (subscription.getEnableMessageOrdering()) {
      throw new IllegalStateException(
          "Attempted to create a MessageBacklog from a subscription with message ordering"
              + " enabled.");
    }
  }

  MessageBacklog(
      String subscriptionName,
      String topicName,
      Duration ackDeadline,
      AtomicLong ackIdProvider,
      RedeliveryPolicy redeliveryPolicy,
      Clock clock) {
    this.subscriptionName = subscriptionName;
    this.topicName = topicName;
    this.ackDeadline = ackDeadline;
    this.ackIdProvider = ackIdProvider;
    this.clock = clock;
    this.redeliveryPolicy = redeliveryPolicy;
    messages = new LinkedHashMap<>();
    ackSet = new HashSet<>();
    undelivered = new LinkedHashSet<>();
    delivered = new LinkedHashMap<>();
    retained = new HashSet<>();
  }

  /** Adds a list of messages to the collection of undelivered messages. */
  @Override
  public synchronized void addAll(List<PubsubMessage> newMessages) {
    for (PubsubMessage message : newMessages) {
      String messageId = message.getMessageId();
      messages.put(messageId, message);
      undelivered.add(messageId);
    }
  }

  /**
   * Moves at most <code>maxMessages</code> messages from the collection of undelivered messages to
   * the collection of delivered messages, and resets delivered messages whose ack deadlines have
   * expired.
   */
  @Override
  public List<ReceivedMessage> pull(int maxMessages) {
    return pull(maxMessages, ackDeadline);
  }

  /**
   * Moves at most <code>maxMessages</code> messages from the collection of undelivered messages to
   * the collection of delivered messages, and resets delivered messages whose ack deadlines have
   * expired. The delivered messages expire in <code>deadline</code>.
   */
  @Override
  public synchronized List<ReceivedMessage> pull(int maxMessages, Duration deadline) {
    List<ReceivedMessage> results = new ArrayList<>();
    maxMessages = Math.min(maxMessages, MAX_MESSAGES_PER_PULL_REQUEST);
    long currentTimeMillis = clock.currentTimeMillis();
    updateExpiredAckDeadlines(currentTimeMillis);

    Iterator<String> it = undelivered.iterator();
    while (it.hasNext()) {
      if (results.size() >= maxMessages) {
        break;
      }
      String messageId = it.next();
      PubsubMessage message = messages.get(messageId);
      String ackId = new AckId(subscriptionName, ackIdProvider.getAndIncrement()).toString();
      delivered.put(ackId, new DeliveryInfo(messageId, currentTimeMillis + deadline.toMillis()));
      it.remove();
      results.add(ReceivedMessage.newBuilder().setMessage(message).setAckId(ackId).build());
    }

    return results;
  }

  @Override
  public List<ReceivedMessage> streamingPull(long streamId, int maxMessages) {
    return pull(maxMessages, ackDeadline);
  }

  @Override
  public List<ReceivedMessage> streamingPull(long streamId, int maxMessages, Duration deadline) {
    return pull(maxMessages, deadline);
  }

  @Override
  public void onStreamingPullCompleted(long streamId) {
    // No op.
  }

  @Override
  public synchronized int getDeliveredCount() {
    return delivered.size();
  }

  /**
   * Acknowledges messages corresponding to <code>ackIds</code> by removing their delivery info from
   * delivered and adding their message IDs to the ack set. Returns <code>INVALID_ARGUMENT</code> if
   * an ack specifies the wrong subscription. Ignores unknown <code>ackIds</code>.
   */
  @Override
  public synchronized void acknowledge(List<String> ackIds) throws StatusException {
    // First validate all ackIds.
    Set<String> validAckIds = new HashSet<>();
    for (String ackIdText : ackIds) {
      AckId ackId = AckId.parseFrom(ackIdText);
      if (!ackId.subscriptionName.equals(subscriptionName)) {
        throw Status.INVALID_ARGUMENT
            .withDescription("Some ack_id values are for another subscription")
            .asException();
      } else if (delivered.containsKey(ackIdText)) {
        validAckIds.add(ackIdText);
      }
    }

    for (String validAckId : validAckIds) {
      String messageId = delivered.get(validAckId).getMessageId();
      if (retainAckedMessages) {
        retained.add(messageId);
      }
      ackSet.add(messageId);
      delivered.remove(validAckId);
    }
  }

  /**
   * Acknowledges messages whose predecessors are all acked. Marks the messages associated with
   * <code>ackIds</code> as "acknowledged" rather than actually removing them from the delivered
   * collection. Then, if there are messages whose predecessors are all acked, acknowledges them
   * together. Therefore, if any old unacked messages exist, the given messages will not be actually
   * acked, but will be only marked as acked.
   */
  @Override
  public synchronized void cumulativeAcknowledge(List<String> ackIds) throws StatusException {
    // Mark as acknowledged, but do not actually acknowledge the messages yet.
    for (String ackId : ackIds) {
      DeliveryInfo info = delivered.get(ackId);
      if (info != null) {
        info.setAcknowledged();
      }
    }

    // Iterate through delivered from the oldest message ID, and find consecutive messages marked as
    // acknowledged.
    List<String> ackIdsToBeAcked = new ArrayList<>();
    for (Map.Entry<String, DeliveryInfo> entry : delivered.entrySet()) {
      // If unacked message is found, stop adding messages to the ack list.
      if (!entry.getValue().isAcknowledged()) {
        break;
      }
      ackIdsToBeAcked.add(entry.getKey());
    }

    acknowledge(ackIdsToBeAcked);
  }

  /**
   * Modifies the ack deadline for the delivered messages specified by <code>ackIds</code> to <code>
   * ackDeadlinesMillis</code> from the present. Returns <code>INVALID_ARGUMENT</code> if an ack
   * specifies the wrong subscription.
   */
  @Override
  public synchronized void modifyAckDeadline(List<String> ackIds, List<Long> ackDeadlinesMillis)
      throws StatusException {
    for (String ackId : ackIds) {
      AckId parsedAckId = AckId.parseFrom(ackId);
      if (!parsedAckId.subscriptionName.equals(subscriptionName)) {
        throw Status.INVALID_ARGUMENT.withDescription("Invalid ack id").asException();
      }
    }
    for (int i = 0; i < ackIds.size(); i++) {
      String ackId = ackIds.get(i);
      long ackDeadlineMillis = ackDeadlinesMillis.get(i);

      if (!delivered.containsKey(ackId)) {
        continue;
      }
      DeliveryInfo info = delivered.get(ackId);
      if (ackDeadlineMillis == 0) {
        // Nack the message.
        undelivered.add(info.getMessageId());
        delivered.remove(ackId);
      } else {
        info.setExpirationMillis(clock.currentTimeMillis() + ackDeadlineMillis);
      }
    }
  }

  /** Removes messages from <code>delivered</code> if their ack deadlines have elapsed. */
  private void updateExpiredAckDeadlines(long nowMillis) {
    if (delivered.isEmpty()) {
      return;
    }
    Iterator<Map.Entry<String, DeliveryInfo>> it = delivered.entrySet().iterator();
    LinkedHashSet<String> newUndelivered = new LinkedHashSet<>();
    if (redeliveryPolicy.equals(RedeliveryPolicy.RESEND_ALL)) {
      // When the redelivery policy is RESEND_ALL, only check if the ack deadline of the first
      // message has expired instead of checking every message separately. If the first message is
      // expired, move all the messages in `delivered` to `undelivered` regardless of the ack
      // deadline of those messages. This mechanism prevents gaps between redelivered messages.
      DeliveryInfo first = it.next().getValue();
      if (first.getExpirationMillis() <= nowMillis) {
        newUndelivered.add(first.getMessageId());
        while (it.hasNext()) {
          String messageId = it.next().getValue().getMessageId();
          newUndelivered.add(messageId);
        }
        delivered.clear();
      }
    } else {
      while (it.hasNext()) {
        DeliveryInfo info = it.next().getValue();
        if (info.getExpirationMillis() <= nowMillis) {
          newUndelivered.add(info.getMessageId());
          it.remove();
        }
      }
    }

    newUndelivered.addAll(undelivered);
    undelivered = newUndelivered;
  }

  /** Creates and returns a snapshot of this backlog. */
  @Override
  public synchronized SnapshotData createSnapshot(String name) throws StatusException {
    return new SnapshotData(
        name, topicName, /*isOrdered=*/ false, ackSet, oldestUnackedPublishTime(), clock);
  }

  /**
   * Returns the publish time of the oldest unacked message in milliseconds. If there are no unacked
   * messages, returns Instant.MAX.
   */
  Instant oldestUnackedPublishTime() {
    Instant oldestUnackedPublishTime = Instant.MAX;

    // `delivered` is not ordered by publish time, so we have to iterate through all the values.
    for (Map.Entry<String, DeliveryInfo> entry : delivered.entrySet()) {
      String messageId = entry.getValue().getMessageId();
      Instant publishTime =
          JavaTimeConversions.toJavaInstant(messages.get(messageId).getPublishTime());
      if (publishTime.isBefore(oldestUnackedPublishTime)) {
        oldestUnackedPublishTime = publishTime;
      }
    }

    Iterator<String> undeliveredIter = undelivered.iterator();
    // `undelivered` is ordered by publish time, so we only need to look at the first value.
    if (undeliveredIter.hasNext()) {
      String messageId = undeliveredIter.next();
      Instant publishTime =
          JavaTimeConversions.toJavaInstant(messages.get(messageId).getPublishTime());
      if (publishTime.isBefore(oldestUnackedPublishTime)) {
        oldestUnackedPublishTime = publishTime;
      }
    }

    return oldestUnackedPublishTime;
  }

  /**
   * Performs a seek-to-snapshot on this backlog to <code>snapshot</code>. Ignores message IDs
   * present in the ack set of <code>snapshot</code> but not present in this backlog.
   */
  @Override
  public synchronized void seek(SnapshotData snapshot) throws StatusException {
    ImmutableSet<String> snapshotAckSet = snapshot.getAckSet();
    Set<String> newAckSet = new HashSet<>(messages.keySet());
    newAckSet.retainAll(snapshotAckSet);

    seek(newAckSet);
  }

  /** Performs a seek-to-time on this backlog to <code>time</code>. */
  @Override
  public synchronized void seek(Instant time) {
    Set<String> newAckSet = new HashSet<>();

    // Iterate through <code>messages</code>, which is sorted by publish time.
    for (PubsubMessage message : messages.values()) {
      Instant publishTime = JavaTimeConversions.toJavaInstant(message.getPublishTime());
      String messageId = message.getMessageId();
      // Ack all messages that were:
      // - published before the seek time
      // - published at or after the seek time and were acked but not retained
      if (publishTime.isBefore(time)
          || (ackSet.contains(messageId) && !retained.contains(messageId))) {
        newAckSet.add(messageId);
      }
    }

    seek(newAckSet);
  }

  /**
   * Recalculates this backlog's ack set to be the same as <code>ackSet</code>, and fixes up the
   * rest of this backlog's state accordingly. We must ensure that the following conditions are
   * satisfied immediately post-seek:
   *
   * <p>1. <code>this.ackSet</code> contains all acked messages (determined by the seek).
   *
   * <p>2. <code>this.undelivered</code> contains all undelivered messages (determined by the seek),
   * ordered by publish time.
   *
   * <p>3. <code>this.delivered</code> is empty.
   *
   * <p>4. All messages in <code>messages</code> are in exactly one of <code>this.ackSet</code> or
   * <code>this.undelivered</code>.
   */
  private void seek(Set<String> newAckSet) {
    // Post-seek, all messages are either acked or undelivered.
    delivered.clear();
    // This constructor ensures `newUndelivered` is ordered the same way as `messages`, i.e. by
    // publish time.
    LinkedHashSet<String> newUndelivered = new LinkedHashSet<>(messages.keySet());
    newUndelivered.removeAll(newAckSet);

    undelivered = newUndelivered;
    ackSet = newAckSet;
  }

  @Override
  public synchronized void setRetainAckedMessages(boolean retainAckedMessages) {
    this.retainAckedMessages = retainAckedMessages;
  }

  @Override
  public synchronized void setAckDeadline(Duration ackDeadline) {
    this.ackDeadline = ackDeadline;
  }

  @Override
  public boolean messageOrderingEnabled() {
    // Return false because `ordering_key` is ignored in this class.
    return false;
  }

  public Set<String> getAckSet() {
    return ackSet;
  }

  /**
   * An identifier for a delivered message requiring acknowledgment. The textual format of the
   * identifier is non-standard.
   */
  static class AckId {
    final String subscriptionName;
    final long instance;

    /**
     * Parses and returns an acknowledgement id represented as a String.
     *
     * @param ackId the value to parse
     * @throws StatusException if the ackId is invalid. The exception carries INVALID_ARGUMENT as
     *     its status.
     */
    static AckId parseFrom(String ackId) throws StatusException {
      int splitIndex = ackId.lastIndexOf(':');
      if (splitIndex != -1 && splitIndex + 1 < ackId.length()) {
        String subscriptionName = ackId.substring(0, splitIndex);
        Long instance = Longs.tryParse(ackId.substring(splitIndex + 1));
        if (!subscriptionName.isEmpty() && instance != null) {
          return new AckId(subscriptionName, instance);
        }
      }
      throw Status.INVALID_ARGUMENT
          .withDescription("Invalid ack id (ack_id=" + ackId + ")")
          .asException();
    }

    AckId(String subscriptionName, long instance) {
      this.subscriptionName = subscriptionName;
      this.instance = instance;
    }

    @Override
    public String toString() {
      return subscriptionName + ":" + instance;
    }
  }

  /** Acknowledgement and expiration info about a delivered message. */
  private static class DeliveryInfo {
    private final String messageId;
    private long expirationMillis;
    private boolean acknowledged;

    public DeliveryInfo(String messageId, long expirationMillis) {
      this.messageId = messageId;
      this.expirationMillis = expirationMillis;
      this.acknowledged = false;
    }

    String getMessageId() {
      return messageId;
    }

    void setExpirationMillis(long expirationMillis) {
      this.expirationMillis = expirationMillis;
    }

    long getExpirationMillis() {
      return expirationMillis;
    }

    boolean isAcknowledged() {
      return acknowledged;
    }

    void setAcknowledged() {
      acknowledged = true;
    }
  }
}
