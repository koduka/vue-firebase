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

import com.google.cloud.iam.testing.v1.shared.authorization.AuthorizationHelper;
import com.google.cloud.iam.testing.v1.shared.internalapi.IamInternalService;
import com.google.cloud.iam.testing.v1.shared.internalapi.protos.GetPolicyRequest;
import com.google.cloud.iam.testing.v1.shared.internalapi.protos.GetPolicyResponse;
import com.google.cloud.iam.testing.v1.shared.internalapi.protos.SetPolicyRequest;
import com.google.cloud.iam.testing.v1.shared.internalapi.protos.SetPolicyResponse;
import com.google.cloud.iam.testing.v1.shared.internalapi.protos.TestPermissionsRequest;
import com.google.cloud.iam.testing.v1.shared.internalapi.protos.TestPermissionsResponse;
import com.google.cloud.iam.testing.v1.shared.permissions.PubsubPermissions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.SettableFuture;
import com.google.iam.v1.GetIamPolicyRequest;
import com.google.iam.v1.Policy;
import com.google.iam.v1.SetIamPolicyRequest;
import com.google.iam.v1.TestIamPermissionsRequest;
import com.google.iam.v1.TestIamPermissionsResponse;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.CreateSnapshotRequest;
import com.google.pubsub.v1.DeleteSnapshotRequest;
import com.google.pubsub.v1.DeleteSubscriptionRequest;
import com.google.pubsub.v1.DeleteTopicRequest;
import com.google.pubsub.v1.GetSnapshotRequest;
import com.google.pubsub.v1.GetSubscriptionRequest;
import com.google.pubsub.v1.GetTopicRequest;
import com.google.pubsub.v1.ListSnapshotsRequest;
import com.google.pubsub.v1.ListSnapshotsResponse;
import com.google.pubsub.v1.ListSubscriptionsRequest;
import com.google.pubsub.v1.ListSubscriptionsResponse;
import com.google.pubsub.v1.ListTopicSnapshotsRequest;
import com.google.pubsub.v1.ListTopicSnapshotsResponse;
import com.google.pubsub.v1.ListTopicSubscriptionsRequest;
import com.google.pubsub.v1.ListTopicSubscriptionsResponse;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ListTopicsResponse;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.ModifyPushConfigRequest;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SeekRequest;
import com.google.pubsub.v1.SeekResponse;
import com.google.pubsub.v1.Snapshot;
import com.google.pubsub.v1.StreamingPullRequest;
import com.google.pubsub.v1.StreamingPullResponse;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.UpdateSubscriptionRequest;
import io.gapi.emulators.util.Observer;
import io.gapi.emulators.util.Resettable;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import javax.annotation.concurrent.GuardedBy;

/**
 * A fake Pubsub server which behaves much like the live Pubsub service, in accordance with the
 * Pubsub API. Does not implement the separate concerns of authentication, ACL-checks, rate
 * limiting, etc. Thread-safe.
 */
class FakePubsubServer
    implements PublisherService, SubscriberService, IamPolicyService, Resettable {
  // This maximum may not reflect the maximum enforced by the actual service.
  private static final int MAX_MESSAGES_PER_PUBLISH_REQUEST = 1000;

  // The default value of Subscription.ack_deadline_seconds, if none is specified.
  static final int DEFAULT_ACK_DEADLINE_SECS = 10;

  static final Duration MIN_RETENTION_DURATION = Durations.fromMinutes(10);
  static final Duration MAX_RETENTION_DURATION = Durations.fromDays(7);
  static final Duration DEFAULT_RETENTION_DURATION = MAX_RETENTION_DURATION;

  // This maximum may not reflect the maximum enforced by the actual service.
  static final int MAX_ACK_DEADLINE_SECS = 600;

  // The default page size for list operations that support paging.
  private static final int DEFAULT_PAGE_SIZE = 1000;

  // The internal sleep time when waiting inside a blocking pull operation.
  @VisibleForTesting static final long HANGING_PULL_POLLING_INTERVAL_MS = 50;

  // The maximum number of outstanding push requests for a given subscription.
  private static final int NUM_CONCURRENT_PUSH_REQUESTS = 2;

  // Resource names.
  private static final String TOPICS_COLLECTION_NAME = "topics";
  private static final String SUBSCRIPTIONS_COLLECTION_NAME = "subscriptions";
  private static final String SNAPSHOTS_COLLECTION_NAME = "snapshots";

  // Topic name for subscriptions to deleted topics.
  private static final String DELETED_TOPIC = "_deleted-topic_";

  // Mutable update fields for UpdateSubscription.
  private static final ImmutableSet<String> MUTABLE_SUBSCRIPTION_FIELDS =
      ImmutableSet.of(
          "retain_acked_messages",
          "ack_deadline_seconds",
          "push_config",
          "message_retention_duration");
  // Immutable update fields for UpdateSubscription.
  private static final ImmutableSet<String> IMMUTABLE_SUBSCRIPTION_FIELDS =
      ImmutableSet.of("name", "topic", "enable_message_ordering");
  // Unsupported update fields for UpdateSubscription.
  private static final ImmutableSet<String> UNSUPPORTED_SUBSCRIPTION_FIELDS =
      ImmutableSet.of("expiration_policy", "dead_letter_policy", "initial_snapshot", "filter");

  // Matches both http and https endpoints.
  @VisibleForTesting
  static final Pattern HTTP_ENDPOINT_PATTERN = Pattern.compile("^(?:http|https)://");

  private final ImmutableSet<String> validProjects;

  // Prefer concurrent maps to favor concurrency over memory efficiency (e.g. TreeMap with
  // added synchronization).
  private final ConcurrentSkipListMap<String, Topic> topics;
  private final ConcurrentMap<String, SubscriptionData> subscriptions;
  private final ConcurrentMap<String, SnapshotData> snapshots;
  private final AtomicLong nextMessageId;
  private final AtomicLong nextAckId;
  private final AtomicLong nextStreamId;

  // Bookkeeping for harmonizing resets with hanging pulls.
  private final Lock resetLock;

  @GuardedBy("resetLock")
  private boolean resetting;

  private final Condition notResetting;

  @GuardedBy("resetLock")
  private int hangingPulls;

  private final Condition noHangingPulls;

  private final EndpointPusherRegistry pusherRegistry;

  // Push delivery and pull completion are performed on this executor.
  private final ExecutorService executor;

  private final IamInternalService iamInternal;

  private final AuthorizationHelper authorizationHelper;

  /**
   * @param validProjects the set of valid project names. If empty, all projects are considered
   *     valid.
   * @param iamInternalService the client used to talk to the IAM fake.
   * @param authorizationHelper the helper used to perform permission checks and retrieve tokens
   *     from the current request.
   */
  FakePubsubServer(
      Collection<String> validProjects,
      IamInternalService iamInternalService,
      AuthorizationHelper authorizationHelper) {
    Preconditions.checkNotNull(iamInternalService);
    Preconditions.checkNotNull(authorizationHelper);

    this.validProjects = ImmutableSet.copyOf(validProjects);
    this.iamInternal = iamInternalService;
    this.authorizationHelper = authorizationHelper;
    topics = new ConcurrentSkipListMap<>();
    subscriptions = new ConcurrentHashMap<>();
    snapshots = new ConcurrentHashMap<>();
    nextMessageId = new AtomicLong(1);
    nextAckId = new AtomicLong(1);
    nextStreamId = new AtomicLong(1);
    resetLock = new ReentrantLock();
    resetting = false;
    notResetting = resetLock.newCondition();
    hangingPulls = 0;
    noHangingPulls = resetLock.newCondition();
    pusherRegistry = new EndpointPusherRegistry();
    executor = Executors.newCachedThreadPool();

    pusherRegistry.register(HTTP_ENDPOINT_PATTERN, new HttpEndpointPusher());
  }

  @Override
  public Topic createTopic(Topic topic) throws StatusException {
    checkProjectValidity(topic.getName(), TOPICS_COLLECTION_NAME);

    String projectPolicy =
        getProjectPolicy(ProjectAndResourceKey.parseFrom(topic.getName(), TOPICS_COLLECTION_NAME));
    authorizationHelper.requirePermissions(
        Context.current(), projectPolicy, PubsubPermissions.TOPICS_CREATE);

    if (topics.putIfAbsent(topic.getName(), topic) != null) {
      throw Status.ALREADY_EXISTS.withDescription("Topic already exists").asException();
    }
    // We send the same data in the response as received in the request.
    return topic;
  }

  @Override
  public PublishResponse publish(PublishRequest request) throws StatusException {
    checkProjectValidity(request.getTopic(), TOPICS_COLLECTION_NAME);

    String topicPolicy =
        getPolicyName(ProjectAndResourceKey.parseFrom(request.getTopic(), TOPICS_COLLECTION_NAME));
    authorizationHelper.requirePermissions(
        Context.current(), topicPolicy, PubsubPermissions.TOPICS_PUBLISH);

    if (!topics.containsKey(request.getTopic())) {
      throw Status.NOT_FOUND.withDescription("Topic not found").asException();
    }
    if (request.getMessagesCount() == 0) {
      throw Status.INVALID_ARGUMENT.withDescription("No messages to publish").asException();
    }
    if (request.getMessagesCount() > MAX_MESSAGES_PER_PUBLISH_REQUEST) {
      throw Status.INVALID_ARGUMENT.withDescription("Too many messages to publish").asException();
    }

    PublishResponse.Builder builder = PublishResponse.newBuilder();
    List<PubsubMessage> messages = new ArrayList<>();

    for (PubsubMessage message : request.getMessagesList()) {
      if (message.getData().isEmpty() && message.getAttributesMap().isEmpty()) {
        throw Status.INVALID_ARGUMENT.withDescription("Some messages are empty").asException();
      }
      String messageId = Long.toString(nextMessageId.getAndIncrement());
      messages.add(
          PubsubMessage.newBuilder(message)
              .setMessageId(messageId)
              .setPublishTime(Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000))
              .build());
      builder.addMessageIds(messageId);
    }
    for (SubscriptionData subscription : subscriptions.values()) {
      if (subscription.getConfig().getTopic().equals(request.getTopic())) {
        subscription.getBacklog().addAll(messages);
      }
    }

    return builder.build();
  }

  @Override
  public Topic getTopic(GetTopicRequest request) throws StatusException {
    checkProjectValidity(request.getTopic(), TOPICS_COLLECTION_NAME);

    String topicPolicy =
        getPolicyName(ProjectAndResourceKey.parseFrom(request.getTopic(), TOPICS_COLLECTION_NAME));
    authorizationHelper.requirePermissions(
        Context.current(), topicPolicy, PubsubPermissions.TOPICS_GET);

    Topic topic = topics.get(request.getTopic());
    if (topic == null) {
      throw Status.NOT_FOUND.withDescription("Topic not found").asException();
    }
    return topic;
  }

  @Override
  public ListTopicsResponse listTopics(ListTopicsRequest request) throws StatusException {
    final int pageSize = request.getPageSize() > 0 ? request.getPageSize() : DEFAULT_PAGE_SIZE;
    ListTopicsResponse.Builder builder = ListTopicsResponse.newBuilder();

    String projectPolicy = getProjectPolicy(request.getProject());
    authorizationHelper.requirePermissions(
        Context.current(), projectPolicy, PubsubPermissions.TOPICS_LIST);

    SortedMap<String, Topic> topicsToList = topics.tailMap(request.getPageToken());
    for (Topic topic : topicsToList.values()) {
      if (topic.getName().startsWith(request.getProject() + "/")) {
        if (builder.getTopicsCount() >= pageSize) {
          builder.setNextPageToken(topic.getName());
          break;
        }
        builder.addTopics(topic);
      }
    }

    return builder.build();
  }

  @Override
  public ListTopicSubscriptionsResponse listTopicSubscriptions(
      ListTopicSubscriptionsRequest request) throws StatusException {
    checkProjectValidity(request.getTopic(), TOPICS_COLLECTION_NAME);
    String topicPolicy =
        getPolicyName(ProjectAndResourceKey.parseFrom(request.getTopic(), TOPICS_COLLECTION_NAME));
    authorizationHelper.requirePermissions(
        Context.current(), topicPolicy, PubsubPermissions.TOPICS_GET);

    final int pageSize = request.getPageSize() > 0 ? request.getPageSize() : DEFAULT_PAGE_SIZE;
    // We must sort the subscriptions for consistency when results are returned in multiple pages.
    // The concurrent map does not impose a meaningful ordering.
    SortedSet<String> matchingSubscriptions = new TreeSet<>();

    for (SubscriptionData subscription : subscriptions.values()) {
      if (subscription.getConfig().getTopic().equals(request.getTopic())) {
        matchingSubscriptions.add(subscription.getConfig().getName());
      }
    }
    if (!request.getPageToken().isEmpty()) {
      matchingSubscriptions = matchingSubscriptions.tailSet(request.getPageToken());
    }

    ListTopicSubscriptionsResponse.Builder builder = ListTopicSubscriptionsResponse.newBuilder();
    for (String subscription : matchingSubscriptions) {
      if (builder.getSubscriptionsCount() >= pageSize) {
        builder.setNextPageToken(subscription);
        break;
      }
      builder.addSubscriptions(subscription);
    }

    return builder.build();
  }

  @Override
  public ListTopicSnapshotsResponse listTopicSnapshots(ListTopicSnapshotsRequest request)
      throws StatusException {
    checkProjectValidity(request.getTopic(), TOPICS_COLLECTION_NAME);
    String topicPolicy =
        getPolicyName(ProjectAndResourceKey.parseFrom(request.getTopic(), TOPICS_COLLECTION_NAME));
    authorizationHelper.requirePermissions(
        Context.current(), topicPolicy, PubsubPermissions.TOPICS_GET);

    final int pageSize = request.getPageSize() > 0 ? request.getPageSize() : DEFAULT_PAGE_SIZE;
    SortedSet<String> matchingSnapshots = new TreeSet<>();

    for (SnapshotData snapshot : snapshots.values()) {
      Snapshot snapshotConfig = snapshot.getConfig();
      if (snapshotConfig.getTopic().equals(request.getTopic())) {
        matchingSnapshots.add(snapshotConfig.getName());
      }
    }
    if (!request.getPageToken().isEmpty()) {
      matchingSnapshots = matchingSnapshots.tailSet(request.getPageToken());
    }

    ListTopicSnapshotsResponse.Builder builder = ListTopicSnapshotsResponse.newBuilder();
    for (String snapshot : matchingSnapshots) {
      if (builder.getSnapshotsCount() >= pageSize) {
        builder.setNextPageToken(snapshot);
        break;
      }
      builder.addSnapshots(snapshot);
    }

    return builder.build();
  }

  @Override
  public void deleteTopic(DeleteTopicRequest request) throws StatusException {
    checkProjectValidity(request.getTopic(), TOPICS_COLLECTION_NAME);
    String topicPolicy =
        getPolicyName(ProjectAndResourceKey.parseFrom(request.getTopic(), TOPICS_COLLECTION_NAME));
    authorizationHelper.requirePermissions(
        Context.current(), topicPolicy, PubsubPermissions.TOPICS_DELETE);

    Topic removed = topics.remove(request.getTopic());
    if (removed == null) {
      throw Status.NOT_FOUND.withDescription("Topic not found").asException();
    }
    for (SubscriptionData subscription : subscriptions.values()) {
      if (subscription.getConfig().getTopic().equals(request.getTopic())) {
        subscription.markTopicDeleted();
      }
    }
  }

  @Override
  public Subscription createSubscription(Subscription subscription) throws StatusException {
    checkProjectValidity(subscription.getName(), SUBSCRIPTIONS_COLLECTION_NAME);
    checkProjectValidity(subscription.getTopic(), TOPICS_COLLECTION_NAME);
    checkPushEndpointValidity(subscription.getPushConfig());

    ProjectAndResourceKey topicKey =
        ProjectAndResourceKey.parseFrom(subscription.getTopic(), TOPICS_COLLECTION_NAME);
    authorizationHelper.requirePermissions(
        Context.current(), getProjectPolicy(topicKey), PubsubPermissions.SUBSCRIPTIONS_CREATE);
    authorizationHelper.requirePermissions(
        Context.current(), getPolicyName(topicKey), PubsubPermissions.TOPICS_ATTACH_SUBSCRIPTION);

    validateAckDeadlineSeconds(subscription.getAckDeadlineSeconds());
    if (subscriptions.containsKey(subscription.getName())) {
      throw Status.ALREADY_EXISTS.withDescription("Subscription already exists").asException();
    }
    if (subscription.getTopic().isEmpty()) {
      throw Status.INVALID_ARGUMENT.withDescription("No topic specified").asException();
    }
    if (!topics.containsKey(subscription.getTopic())) {
      throw Status.NOT_FOUND.withDescription("Subscription topic does not exist").asException();
    }
    if (subscription.hasMessageRetentionDuration()) {
      validateMessageRetentionDuration(subscription.getMessageRetentionDuration());
    }

    Subscription.Builder builder = Subscription.newBuilder(subscription);
    if (!subscription.hasPushConfig()) {
      // An empty PushConfig message is echoed back when the value is absent from the request.
      builder.getPushConfigBuilder();
    }
    if (subscription.getAckDeadlineSeconds() == 0) {
      builder.setAckDeadlineSeconds(DEFAULT_ACK_DEADLINE_SECS);
    }
    if (!subscription.hasMessageRetentionDuration()) {
      builder.setMessageRetentionDuration(DEFAULT_RETENTION_DURATION);
    }
    subscription = builder.build();
    subscriptions.put(
        subscription.getName(),
        new SubscriptionData(subscription, nextAckId, pusherRegistry, executor));
    return subscription;
  }

  private static void validateMessageRetentionDuration(Duration retentionDuration)
      throws StatusException {
    if (!Durations.isValid(retentionDuration)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("invalid message_retention_duration")
          .asException();
    }
    if (Durations.compare(retentionDuration, MIN_RETENTION_DURATION) < 0
        || Durations.compare(retentionDuration, MAX_RETENTION_DURATION) > 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("message_retention_duration out of bounds")
          .asException();
    }
  }

  private static void validateAckDeadlineSeconds(int ackDeadlineSeconds) throws StatusException {
    if (ackDeadlineSeconds < 0 || ackDeadlineSeconds > MAX_ACK_DEADLINE_SECS) {
      throw Status.INVALID_ARGUMENT
          .withDescription("ack_deadline_secs out of bounds")
          .asException();
    }
  }

  @Override
  public Subscription getSubscription(GetSubscriptionRequest request) throws StatusException {
    checkProjectValidity(request.getSubscription(), SUBSCRIPTIONS_COLLECTION_NAME);
    String subscriptionPolicy =
        getPolicyName(
            ProjectAndResourceKey.parseFrom(
                request.getSubscription(), SUBSCRIPTIONS_COLLECTION_NAME));
    authorizationHelper.requirePermissions(
        Context.current(), subscriptionPolicy, PubsubPermissions.SUBSCRIPTIONS_GET);

    SubscriptionData subscription = subscriptions.get(request.getSubscription());
    if (subscription == null) {
      throw Status.NOT_FOUND.withDescription("Subscription does not exist").asException();
    }
    return subscription.getConfig();
  }

  @Override
  public ListSubscriptionsResponse listSubscriptions(ListSubscriptionsRequest request)
      throws StatusException {
    String subscriptionPolicy = getProjectPolicy(request.getProject());
    authorizationHelper.requirePermissions(
        Context.current(), subscriptionPolicy, PubsubPermissions.SUBSCRIPTIONS_LIST);

    final int pageSize = request.getPageSize() > 0 ? request.getPageSize() : DEFAULT_PAGE_SIZE;
    // We must sort the subscriptions for consistency when results are returned in multiple pages.
    // The concurrent map does not impose a meaningful ordering.
    SortedMap<String, Subscription> matchingSubscriptions = new TreeMap<>();

    for (SubscriptionData subscription : subscriptions.values()) {
      if (subscription.getConfig().getName().startsWith(request.getProject() + "/")) {
        matchingSubscriptions.put(subscription.getConfig().getName(), subscription.getConfig());
      }
    }
    if (!request.getPageToken().isEmpty()) {
      matchingSubscriptions = matchingSubscriptions.tailMap(request.getPageToken());
    }

    ListSubscriptionsResponse.Builder builder = ListSubscriptionsResponse.newBuilder();
    for (Subscription subscription : matchingSubscriptions.values()) {
      if (builder.getSubscriptionsCount() >= pageSize) {
        builder.setNextPageToken(subscription.getName());
        break;
      }
      builder.addSubscriptions(subscription);
    }

    return builder.build();
  }

  @Override
  public void deleteSubscription(DeleteSubscriptionRequest request) throws StatusException {
    checkProjectValidity(request.getSubscription(), SUBSCRIPTIONS_COLLECTION_NAME);
    String subscriptionPolicy =
        getPolicyName(
            ProjectAndResourceKey.parseFrom(
                request.getSubscription(), SUBSCRIPTIONS_COLLECTION_NAME));
    authorizationHelper.requirePermissions(
        Context.current(), subscriptionPolicy, PubsubPermissions.SUBSCRIPTIONS_DELETE);

    SubscriptionData removed = subscriptions.remove(request.getSubscription());
    if (removed == null) {
      throw Status.NOT_FOUND.withDescription("Subscription does not exist").asException();
    }
    removed.close();
  }

  @Override
  public Subscription updateSubscription(UpdateSubscriptionRequest request) throws StatusException {
    checkProjectValidity(request.getSubscription().getName(), SUBSCRIPTIONS_COLLECTION_NAME);
    String subscriptionPolicy =
        getPolicyName(
            ProjectAndResourceKey.parseFrom(
                request.getSubscription().getName(), SUBSCRIPTIONS_COLLECTION_NAME));

    SubscriptionData subscription = subscriptions.get(request.getSubscription().getName());
    if (subscription == null) {
      throw Status.NOT_FOUND.withDescription("Subscription not found.").asException();
    }
    authorizationHelper.requirePermissions(
        Context.current(), subscriptionPolicy, PubsubPermissions.SUBSCRIPTIONS_UPDATE);

    Set<String> fields = new HashSet<>(request.getUpdateMask().getPathsList());
    if (fields.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "The update_mask in the UpdateSubscriptionRequest must be set, and must contain a"
                  + " non-empty paths list.")
          .asException();
    }
    validateFields(fields);

    Subscription updatedSubscription = request.getSubscription();
    if (fields.contains("retain_acked_messages")) {
      subscription.setRetainAckedMessages(updatedSubscription.getRetainAckedMessages());
    }
    if (fields.contains("ack_deadline_seconds")) {
      int ackDeadlineSeconds;
      if (updatedSubscription.getAckDeadlineSeconds() == 0) {
        ackDeadlineSeconds = DEFAULT_ACK_DEADLINE_SECS;
      } else {
        ackDeadlineSeconds = updatedSubscription.getAckDeadlineSeconds();
        validateAckDeadlineSeconds(ackDeadlineSeconds);
      }
      subscription.setAckDeadline(java.time.Duration.ofSeconds(ackDeadlineSeconds));
    }
    if (fields.contains("push_config")) {
      PushConfig pushConfig = updatedSubscription.getPushConfig();
      checkPushEndpointValidity(pushConfig);
      subscription.setPushConfig(pushConfig);
    }
    if (fields.contains("message_retention_duration")) {
      Duration messageRetentionDuration;
      if (!updatedSubscription.hasMessageRetentionDuration()) {
        messageRetentionDuration = DEFAULT_RETENTION_DURATION;
      } else {
        messageRetentionDuration = updatedSubscription.getMessageRetentionDuration();
        validateMessageRetentionDuration(messageRetentionDuration);
      }
      subscription.setMessageRetentionDuration(messageRetentionDuration);
    }

    return subscription.getConfig();
  }

  private static void validateFields(Set<String> fields) throws StatusException {
    for (String field : fields) {
      if (IMMUTABLE_SUBSCRIPTION_FIELDS.contains(field)) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "Invalid update_mask provided in the UpdateSubscriptionRequest: the "
                    + field
                    + " field in the Subscription is not mutable.")
            .asException();
      } else if (UNSUPPORTED_SUBSCRIPTION_FIELDS.contains(field)) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "Updating the "
                    + field
                    + " field is currently unsupported in the Pub/Sub Emulator.")
            .asException();
      } else if (!MUTABLE_SUBSCRIPTION_FIELDS.contains(field)) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "Invalid update_mask provided in the UpdateSubscriptionRequest: "
                    + field
                    + " is not a known Subscription field. Note that field paths must be of the"
                    + " form 'push_config' rather than 'pushConfig'.")
            .asException();
      }
    }
  }

  @Override
  public Snapshot createSnapshot(CreateSnapshotRequest request) throws StatusException {
    checkProjectValidity(request.getName(), SNAPSHOTS_COLLECTION_NAME);
    String projectPolicy =
        getPolicyName(
            ProjectAndResourceKey.parseFrom(request.getName(), SNAPSHOTS_COLLECTION_NAME));
    authorizationHelper.requirePermissions(
        Context.current(), projectPolicy, PubsubPermissions.SNAPSHOTS_CREATE);

    String name = request.getName();
    if (snapshots.containsKey(name)) {
      throw Status.ALREADY_EXISTS.withDescription("Snapshot already exists").asException();
    }
    SubscriptionData subscription = subscriptions.get(request.getSubscription());
    if (subscription == null) {
      throw Status.NOT_FOUND.withDescription("Subscription does not exist").asException();
    }
    SnapshotData snapshot = subscription.getBacklog().createSnapshot(name);
    snapshots.put(name, snapshot);

    return snapshot.getConfig();
  }

  @Override
  public Snapshot getSnapshot(GetSnapshotRequest request) throws StatusException {
    checkProjectValidity(request.getSnapshot(), SNAPSHOTS_COLLECTION_NAME);
    String snapshotPolicy =
        getPolicyName(
            ProjectAndResourceKey.parseFrom(request.getSnapshot(), SNAPSHOTS_COLLECTION_NAME));
    authorizationHelper.requirePermissions(
        Context.current(), snapshotPolicy, PubsubPermissions.SNAPSHOTS_GET);

    SnapshotData snapshot = snapshots.get(request.getSnapshot());
    if (snapshot == null) {
      throw Status.NOT_FOUND.withDescription("Snapshot does not exist").asException();
    }

    return snapshot.getConfig();
  }

  @Override
  public ListSnapshotsResponse listSnapshots(ListSnapshotsRequest request) throws StatusException {
    String projectPolicy = getProjectPolicy(request.getProject());
    authorizationHelper.requirePermissions(
        Context.current(), projectPolicy, PubsubPermissions.SNAPSHOTS_LIST);

    final int pageSize = request.getPageSize() > 0 ? request.getPageSize() : DEFAULT_PAGE_SIZE;
    SortedMap<String, Snapshot> matchingSnapshots = new TreeMap<>();

    for (SnapshotData snapshot : snapshots.values()) {
      Snapshot snapshotConfig = snapshot.getConfig();
      if (snapshotConfig.getName().startsWith(request.getProject() + "/")) {
        matchingSnapshots.put(snapshotConfig.getName(), snapshotConfig);
      }
    }

    if (!request.getPageToken().isEmpty()) {
      matchingSnapshots = matchingSnapshots.tailMap(request.getPageToken());
    }

    ListSnapshotsResponse.Builder builder = ListSnapshotsResponse.newBuilder();
    for (Snapshot snapshot : matchingSnapshots.values()) {
      if (builder.getSnapshotsCount() >= pageSize) {
        builder.setNextPageToken(snapshot.getName());
        break;
      }
      builder.addSnapshots(snapshot);
    }

    return builder.build();
  }

  @Override
  public void deleteSnapshot(DeleteSnapshotRequest request) throws StatusException {
    checkProjectValidity(request.getSnapshot(), SNAPSHOTS_COLLECTION_NAME);
    String snapshotPolicy =
        getPolicyName(
            ProjectAndResourceKey.parseFrom(request.getSnapshot(), SNAPSHOTS_COLLECTION_NAME));
    authorizationHelper.requirePermissions(
        Context.current(), snapshotPolicy, PubsubPermissions.SNAPSHOTS_DELETE);

    SnapshotData removed = snapshots.remove(request.getSnapshot());
    if (removed == null) {
      throw Status.NOT_FOUND.withDescription("Snapshot does not exist").asException();
    }
  }

  @Override
  public SeekResponse seek(SeekRequest request) throws StatusException {
    checkProjectValidity(request.getSubscription(), SUBSCRIPTIONS_COLLECTION_NAME);
    String subscriptionPolicy =
        getPolicyName(
            ProjectAndResourceKey.parseFrom(
                request.getSubscription(), SUBSCRIPTIONS_COLLECTION_NAME));
    SubscriptionData subscription = subscriptions.get(request.getSubscription());
    if (subscription == null) {
      throw Status.NOT_FOUND.withDescription("Subscription does not exist").asException();
    }
    authorizationHelper.requirePermissions(
        Context.current(), subscriptionPolicy, PubsubPermissions.SUBSCRIPTIONS_CONSUME);

    Backlog backlog = subscription.getBacklog();
    if (request.hasTime()) {
      // Seek to time.
      Timestamp tSeek = request.getTime();
      if (!Timestamps.isValid(tSeek)) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "An invalid time was specified in the SeekRequest. See definition of Timestamp.")
            .asException();
      }
      backlog.seek(JavaTimeConversions.toJavaInstant(tSeek));
    } else if (request.getSnapshot().isEmpty()) {
      // No target specified.
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "No target was specified in the SeekRequest. Must specify either a time or a"
                  + " snapshot")
          .asException();
    } else {
      // Seek to snapshot.
      checkProjectValidity(request.getSnapshot(), SNAPSHOTS_COLLECTION_NAME);
      String snapshotPolicy =
          getPolicyName(
              ProjectAndResourceKey.parseFrom(request.getSnapshot(), SNAPSHOTS_COLLECTION_NAME));
      SnapshotData snapshot = snapshots.get(request.getSnapshot());
      if (snapshot == null) {
        throw Status.NOT_FOUND.withDescription("Snapshot does not exist").asException();
      }
      authorizationHelper.requirePermissions(
          Context.current(), snapshotPolicy, PubsubPermissions.SNAPSHOTS_SEEK);

      if (!snapshot.getConfig().getTopic().equals(subscription.getConfig().getTopic())) {
        throw Status.FAILED_PRECONDITION
            .withDescription(
                "The subscription's topic "
                    + subscription.getConfig().getTopic()
                    + " is different from that of the snapshot "
                    + snapshot.getConfig().getTopic()
                    + "; they must match in order for Seek work. Note that if a topic is deleted"
                    + " and then re-created with the same name, it is considered a distinct topic"
                    + " for these purposes.")
            .asException();
      } else if (snapshot.isExpired()) {
        throw Status.FAILED_PRECONDITION
            .withDescription(
                "The operation could not be completed because the required source subscription's"
                    + " backlog is too old.")
            .asException();
      }
      backlog.seek(snapshot);
    }

    return SeekResponse.getDefaultInstance();
  }

  @Override
  public void modifyAckDeadline(ModifyAckDeadlineRequest request) throws StatusException {
    modifyAckDeadline(
        request.getSubscription(),
        request.getAckIdsList(),
        Collections.nCopies(request.getAckIdsCount(), request.getAckDeadlineSeconds() * 1000L));
  }

  private void modifyAckDeadline(
      String subscriptionName, List<String> ackIds, List<Long> deadlinesMillis)
      throws StatusException {
    checkProjectValidity(subscriptionName, SUBSCRIPTIONS_COLLECTION_NAME);
    String subscriptionPolicy =
        getPolicyName(
            ProjectAndResourceKey.parseFrom(subscriptionName, SUBSCRIPTIONS_COLLECTION_NAME));
    authorizationHelper.requirePermissions(
        Context.current(), subscriptionPolicy, PubsubPermissions.SUBSCRIPTIONS_CONSUME);

    SubscriptionData subscription = subscriptions.get(subscriptionName);
    if (subscription == null) {
      throw Status.NOT_FOUND.withDescription("Subscription does not exist").asException();
    }
    if (ackIds.size() != deadlinesMillis.size()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("ack IDs and deadlines must have the same size")
          .asException();
    }
    if (ackIds.isEmpty()) {
      throw Status.INVALID_ARGUMENT.withDescription("No ack ids specified").asException();
    }
    for (long deadline : deadlinesMillis) {
      if (deadline < 0) {
        throw Status.INVALID_ARGUMENT
            .withDescription("Ack deadline cannot be negative")
            .asException();
      }
    }

    subscription.getBacklog().modifyAckDeadline(ackIds, deadlinesMillis);
  }

  @Override
  public void acknowledge(AcknowledgeRequest request) throws StatusException {
    checkProjectValidity(request.getSubscription(), SUBSCRIPTIONS_COLLECTION_NAME);
    String subscriptionPolicy =
        getPolicyName(
            ProjectAndResourceKey.parseFrom(
                request.getSubscription(), SUBSCRIPTIONS_COLLECTION_NAME));
    authorizationHelper.requirePermissions(
        Context.current(), subscriptionPolicy, PubsubPermissions.SUBSCRIPTIONS_CONSUME);

    SubscriptionData subscription = subscriptions.get(request.getSubscription());
    if (subscription == null) {
      throw Status.NOT_FOUND.withDescription("Subscription does not exist").asException();
    }
    if (request.getAckIdsCount() == 0) {
      throw Status.INVALID_ARGUMENT.withDescription("No ack ids specified.").asException();
    }
    subscription.getBacklog().acknowledge(request.getAckIdsList());
  }

  @SuppressWarnings("FutureReturnValueIgnored") // Runnable handles all checked exceptions
  @Override
  public void pull(
      final PullRequest request,
      final Observer<PullResponse> observer,
      final RpcDeadline deadline) {
    final SubscriptionData subscription;
    try {
      checkProjectValidity(request.getSubscription(), SUBSCRIPTIONS_COLLECTION_NAME);
      String subscriptionPolicy =
          getPolicyName(
              ProjectAndResourceKey.parseFrom(
                  request.getSubscription(), SUBSCRIPTIONS_COLLECTION_NAME));
      authorizationHelper.requirePermissions(
          Context.current(), subscriptionPolicy, PubsubPermissions.SUBSCRIPTIONS_CONSUME);

      subscription = subscriptions.get(request.getSubscription());
      if (subscription == null) {
        throw Status.NOT_FOUND.withDescription("Subscription does not exist").asException();
      }
      if (request.getMaxMessages() == 0) {
        throw Status.INVALID_ARGUMENT.withDescription("No max_messages specified").asException();
      }
    } catch (StatusException error) {
      observer.onError(error);
      return;
    }

    if (request.getReturnImmediately()) {
      observer.onValue(
          PullResponse.newBuilder()
              .addAllReceivedMessages(subscription.getBacklog().pull(request.getMaxMessages()))
              .build());
      return;
    }

    executor.submit(
        new Runnable() {
          @Override
          public void run() {
            registerHangingPull();

            // The try scope is expanded beyond the sleep(), since this thread could be interrupted
            // while not sleeping. It is convenient to conflate the exception handling with the
            // guard for unregistering the hanging pull.
            try {
              while (true) {
                List<ReceivedMessage> newMessages =
                    subscription.getBacklog().pull(request.getMaxMessages());
                // We wait until the deadline is just about to expire, with a safety margin.
                if (!newMessages.isEmpty()
                    || deadline.getRemainingMillis() < 1000
                    || isResetting()) {
                  observer.onValue(
                      PullResponse.newBuilder().addAllReceivedMessages(newMessages).build());
                  return;
                }
                // The fake prefers be simple code rather than optimal, so we do this sleep loop
                // here; this also better simulates the behavior of the Cloud Pub/Sub server.
                Thread.sleep(HANGING_PULL_POLLING_INTERVAL_MS);
              }
            } catch (InterruptedException e) {
              observer.onError(Status.CANCELLED.withCause(e).asException());
            } finally {
              unregisterHangingPull();
            }
          }
        });
  }

  @SuppressWarnings("GuardedByChecker")
  private void registerHangingPull() {
    resetLock.lock();
    try {
      ++hangingPulls;
    } finally {
      resetLock.unlock();
    }
  }

  @SuppressWarnings("GuardedByChecker")
  private void unregisterHangingPull() {
    resetLock.lock();
    try {
      --hangingPulls;
      if (hangingPulls == 0) {
        noHangingPulls.signalAll();
      }
    } finally {
      resetLock.unlock();
    }
  }

  @Override
  public StreamObserver<StreamingPullRequest> streamingPull(
      StreamObserver<StreamingPullResponse> responseObserver, RpcDeadline deadline) {
    SettableFuture<String> subscriptionNameFuture = SettableFuture.create();
    SettableFuture<Void> terminationFuture = SettableFuture.create();
    AtomicInteger deadlineSecs = new AtomicInteger(10);
    executor.execute(
        new StreamingPullPusher(
            nextStreamId.getAndIncrement(),
            responseObserver,
            subscriptionNameFuture,
            terminationFuture,
            deadlineSecs,
            deadline));
    return new StreamingPullRequestObserver(
        subscriptionNameFuture, terminationFuture, deadlineSecs);
  }

  @Override
  public void modifyPushConfig(ModifyPushConfigRequest request) throws StatusException {
    checkProjectValidity(request.getSubscription(), SUBSCRIPTIONS_COLLECTION_NAME);
    String subscriptionPolicy =
        getPolicyName(
            ProjectAndResourceKey.parseFrom(
                request.getSubscription(), SUBSCRIPTIONS_COLLECTION_NAME));
    authorizationHelper.requirePermissions(
        Context.current(), subscriptionPolicy, PubsubPermissions.SUBSCRIPTIONS_UPDATE);

    checkPushEndpointValidity(request.getPushConfig());
    SubscriptionData subscription = subscriptions.get(request.getSubscription());
    if (subscription == null) {
      throw Status.NOT_FOUND.withDescription("Subscription does not exist").asException();
    }
    subscription.setPushConfig(request.getPushConfig());
  }

  /**
   * Resets the state of this server. When this call completes, it is guaranteed that any hanging
   * pull requests made before or during this call have been completed.
   *
   * <p>Note that as currently implemented, races are possible if other mutating operations are
   * performed concurrently with a call to this method.
   */
  @Override
  public void reset() {
    resetLock.lock();
    try {
      if (resetting) {
        // Another caller came first; it does the work.
        do {
          notResetting.awaitUninterruptibly();
        } while (resetting);
        return;
      }
      resetting = true;

      while (hangingPulls > 0) {
        noHangingPulls.awaitUninterruptibly();
      }

      topics.clear();
      subscriptions.clear();

      resetting = false;
      notResetting.signalAll();
    } finally {
      resetLock.unlock();
    }
  }

  /** Registers a pusher to be used for endpoints with the specified prefix. */
  public void registerEndpointPusher(String endpointRegex, EndpointPusher pusher) {
    pusherRegistry.register(Pattern.compile(endpointRegex), pusher);
  }

  /**
   * Checks the resource path specifies a valid project.
   *
   * @param resourcePath the resource path
   * @param resourceName the resource name
   * @throws StatusException if the resource path is badly formatted or contains an invalid project
   */
  private void checkProjectValidity(String resourcePath, String resourceName)
      throws StatusException {
    ProjectAndResourceKey pt = ProjectAndResourceKey.parseFrom(resourcePath, resourceName);
    if (!validProjects.isEmpty() && !validProjects.contains(pt.project)) {
      throw Status.NOT_FOUND
          .withDescription(
              String.format(
                  "Invalid (project=%s) out of valid projects %s.", pt.project, validProjects))
          .asException();
    }
  }

  /**
   * Check the push configuration has a valid endpoint URL.
   *
   * @param config the push configuration to check.
   * @throws StatusException if the push endpoint is invalid
   */
  private void checkPushEndpointValidity(PushConfig config) throws StatusException {
    final String endpoint = config.getPushEndpoint();
    if (!endpoint.isEmpty()) {
      EndpointPusher pusher = pusherRegistry.getPusher(endpoint);
      if (pusher == null) {
        throw Status.INVALID_ARGUMENT.withDescription("Unsupported push_endpoint").asException();
      }
      try {
        pusher.validateEndpoint(config);
      } catch (IllegalArgumentException e) {
        throw Status.INVALID_ARGUMENT
            .withDescription("Invalid push_endpoint")
            .withCause(e)
            .asException();
      }
    }
  }

  private boolean isResetting() {
    resetLock.lock();
    try {
      return resetting;
    } finally {
      resetLock.unlock();
    }
  }

  /**
   * StreamingPullPusher is responsible for sending PubsubMessages to the client.
   *
   * <p>The listening half must set subscriptionNameFuture to the name of the subscription or an
   * error if the error occurs before the name can be set.
   *
   * <p>After the name is set, the listening half may signal stream termination by setting
   * terminationFuture.
   */
  private class StreamingPullPusher implements Runnable {
    private final long streamId;
    private final StreamObserver<StreamingPullResponse> responseObserver;
    private final Future<String> subscriptionNameFuture;
    private final Future<Void> terminationFuture;
    private final AtomicInteger deadlineSecs;
    private final RpcDeadline streamDeadline;

    private String subscriptionName;
    private String subscriptionId;

    StreamingPullPusher(
        long streamId,
        StreamObserver<StreamingPullResponse> responseObserver,
        Future<String> subscriptionNameFuture,
        Future<Void> terminationFuture,
        AtomicInteger deadlineSecs,
        RpcDeadline streamDeadline) {
      this.streamId = streamId;
      this.responseObserver = responseObserver;
      this.subscriptionNameFuture = subscriptionNameFuture;
      this.terminationFuture = terminationFuture;
      this.deadlineSecs = deadlineSecs;
      this.streamDeadline = streamDeadline;
    }

    private void checkReset() throws StatusException {
      if (isResetting()) {
        throw Status.ABORTED.withDescription("emulator is being reset").asException();
      }
    }

    private void checkSubscription(SubscriptionData subscription) throws StatusException {
      if (subscription == null) {
        throw Status.NOT_FOUND
            .withDescription(
                String.format("Subscription does not exist (resource=%s)", subscriptionId))
            .asException();
      }
      if (!subscription.getPushConfig().getPushEndpoint().isEmpty()) {
        throw Status.FAILED_PRECONDITION
            .withDescription(
                String.format(
                    "Subscription is a push subscription: (resource=%s)", subscriptionName))
            .asException();
      }
      if (streamDeadline.isExpired()) {
        throw Status.DEADLINE_EXCEEDED.asException();
      }
    }

    @Override
    public void run() {
      registerHangingPull();
      try {
        while (!subscriptionNameFuture.isDone()) {
          checkReset();
          Thread.sleep(HANGING_PULL_POLLING_INTERVAL_MS);
        }
        subscriptionName = subscriptionNameFuture.get();

        // Null is sentinel if user closes the stream before we get the subscription.
        if (subscriptionName == null) {
          responseObserver.onCompleted();
          return;
        }
        ProjectAndResourceKey key =
            ProjectAndResourceKey.parseFrom(subscriptionName, SUBSCRIPTIONS_COLLECTION_NAME);
        subscriptionId = key.resourceValue;

        while (!terminationFuture.isDone()) {
          checkReset();
          SubscriptionData subscription = subscriptions.get(subscriptionName);
          checkSubscription(subscription);
          List<ReceivedMessage> newMessages =
              subscription
                  .getBacklog()
                  .streamingPull(streamId, 1_000, java.time.Duration.ofSeconds(deadlineSecs.get()));
          if (!newMessages.isEmpty()) {
            responseObserver.onNext(
                StreamingPullResponse.newBuilder().addAllReceivedMessages(newMessages).build());
          }
          Thread.sleep(HANGING_PULL_POLLING_INTERVAL_MS);
        }
        terminationFuture.get();
      } catch (InterruptedException e) {
        responseObserver.onError(Status.CANCELLED.withCause(e).asException());
        return;
      } catch (Throwable t) {
        responseObserver.onError(Status.fromThrowable(t).asException());
        return;
      } finally {
        unregisterHangingPull();
        SubscriptionData subscription = subscriptions.get(subscriptionName);
        if (subscription != null) {
          subscription.getBacklog().onStreamingPullCompleted(streamId);
        }
      }

      // onCompleted is not in the try block. If onComplete itself throws,
      // we're not allowed to call onError.
      responseObserver.onCompleted();
    }
  }

  private class StreamingPullRequestObserver implements StreamObserver<StreamingPullRequest> {
    private final SettableFuture<String> subscriptionNameFuture;
    private final SettableFuture<Void> terminationFuture;
    private final AtomicInteger ackDeadlineSeconds;
    private String subscriptionName = null;

    StreamingPullRequestObserver(
        SettableFuture<String> subscriptionNameFuture,
        SettableFuture<Void> terminationFuture,
        AtomicInteger ackDeadlineSeconds) {
      this.subscriptionNameFuture = subscriptionNameFuture;
      this.terminationFuture = terminationFuture;
      this.ackDeadlineSeconds = ackDeadlineSeconds;
    }

    private void processPropertyChange(StreamingPullRequest request) throws StatusException {
      // The pusher thread waits for subscriptionNameFuture to complete before pushing.
      // We set deadline before completing the future, so that the deadline value is visible
      // to the pusher before it begins to push.
      if (request.getStreamAckDeadlineSeconds() != 0) {
        if (request.getStreamAckDeadlineSeconds() < 10
            || request.getStreamAckDeadlineSeconds() > 600) {
          throw Status.INVALID_ARGUMENT
              .withDescription("stream_ack_deadline_seconds must be between 10 and 600 seconds")
              .asException();
        }
        ackDeadlineSeconds.set(request.getStreamAckDeadlineSeconds());
      }

      if (subscriptionName == null) {
        // First message, must have subscription.
        if (request.getSubscription().isEmpty()) {
          throw Status.INVALID_ARGUMENT
              .withDescription("first message must set subscription")
              .asException();
        }
        checkProjectValidity(request.getSubscription(), SUBSCRIPTIONS_COLLECTION_NAME);
        subscriptionNameFuture.set(request.getSubscription());
        subscriptionName = request.getSubscription();

        if (request.getStreamAckDeadlineSeconds() == 0) {
          throw Status.INVALID_ARGUMENT
              .withDescription("first message must set stream_ack_deadline_seconds")
              .asException();
        }
      } else if (!request.getSubscription().isEmpty()) {
        throw Status.INVALID_ARGUMENT
            .withDescription("subscription may only be set in first message")
            .asException();
      }
    }

    private void processAcks(StreamingPullRequest request) throws StatusException {
      if (request.getAckIdsCount() != 0) {
        acknowledge(
            AcknowledgeRequest.newBuilder()
                .setSubscription(subscriptionName)
                .addAllAckIds(request.getAckIdsList())
                .build());
      }
    }

    private void processDeadlineModifications(StreamingPullRequest request) throws StatusException {
      // The two lists are required to have the same size, but modifyAckDeadline will check
      // for us.
      if (request.getModifyDeadlineSecondsCount() != 0
          || request.getModifyDeadlineAckIdsCount() != 0) {
        List<Long> deadlinesMillis = new ArrayList<>(request.getModifyDeadlineAckIdsCount());
        for (int deadlineSeconds : request.getModifyDeadlineSecondsList()) {
          deadlinesMillis.add(deadlineSeconds * 1000L);
        }
        modifyAckDeadline(subscriptionName, request.getModifyDeadlineAckIdsList(), deadlinesMillis);
      }
    }

    @Override
    public void onNext(StreamingPullRequest request) {
      try {
        processPropertyChange(request);
        processAcks(request);
        processDeadlineModifications(request);
      } catch (StatusException e) {
        terminationFuture.setException(e);
        subscriptionNameFuture.setException(e);
      }
    }

    @Override
    public void onError(Throwable t) {
      terminationFuture.setException(t);
      subscriptionNameFuture.setException(t);
    }

    @Override
    public void onCompleted() {
      terminationFuture.set(null);
      subscriptionNameFuture.set(null);
    }
  }

  /**
   * A Pubsub project and topic or subscription name, parsed from a full topic or subscription path.
   */
  private static class ProjectAndResourceKey {
    // Matches valid resource values, e.g. both topic and subscription names.
    // See https://cloud.google.com/pubsub/docs/overview#names
    private static final Pattern RESOURCE_PATTERN =
        Pattern.compile("^[A-Za-z][\\w-\\.~\\+%]{2,254}$");

    final String project;
    final String resourceValue;
    final String resourceName;

    /**
     * Parses a resource path. The expected format is
     * "projects/PROJECT/RESOURCE_NAME/RESOURCE_VALUE".
     */
    static ProjectAndResourceKey parseFrom(String resourcePath, String... validResourceNames)
        throws StatusException {
      String[] parts = resourcePath.split("/", 4);
      ImmutableList<String> resourceNamesList = ImmutableList.copyOf(validResourceNames);

      if (parts.length != 4
          || !parts[0].equals("projects")
          || !resourceNamesList.contains(parts[2])
          || !RESOURCE_PATTERN.matcher(parts[3]).matches()
          || parts[3].startsWith("goog")) {
        throw new StatusException(
            Status.INVALID_ARGUMENT.withDescription(
                String.format("Invalid %s name: (name=%s)", resourceNamesList, resourcePath)));
      }
      return new ProjectAndResourceKey(parts[1], parts[2], parts[3]);
    }

    String getResourceUrl() {
      return String.format("projects/%s/%s/%s", project, resourceName, resourceValue);
    }

    private ProjectAndResourceKey(String project, String resourceName, String resourceValue) {
      this.project = project;
      this.resourceName = resourceName;
      this.resourceValue = resourceValue;
    }
  }

  /** A subscription that captures all published messages past a sync point in time. Thread-safe. */
  static class SubscriptionData implements Closeable {
    static final int PUSH_POLLING_INTERVAL_MS = 100;

    private Subscription subscription;
    private final EndpointPusherRegistry pusherRegistry;
    private final Executor pushExecutor;
    private final Backlog backlog;
    private final List<PushLoop> pushLoops;

    SubscriptionData(
        Subscription subscription,
        AtomicLong ackIdProvider,
        EndpointPusherRegistry pusherRegistry,
        Executor pushExecutor) {
      this.subscription = subscription;
      this.pusherRegistry = pusherRegistry;
      this.pushExecutor = pushExecutor;
      if (subscription.getEnableMessageOrdering()) {
        this.backlog = new OrderedMessageBacklog(subscription, ackIdProvider);
      } else {
        this.backlog =
            new MessageBacklog(
                subscription, ackIdProvider, MessageBacklog.RedeliveryPolicy.RESEND_EXPIRED);
      }
      this.pushLoops = new ArrayList<>();
      if (subscription.hasPushConfig()
          && !subscription.getPushConfig().getPushEndpoint().isEmpty()) {
        startPushLoops();
      }
    }

    /** Stops asynchronous work being performed on pushExecutor. */
    @Override
    public void close() {
      stopPushLoops();
    }

    Subscription getConfig() {
      return subscription;
    }

    synchronized void setPushConfig(PushConfig pushConfig) {
      if (pushConfig.equals(subscription.getPushConfig())) {
        return;
      }

      subscription = Subscription.newBuilder(subscription).setPushConfig(pushConfig).build();
      if (pushConfig.getPushEndpoint().isEmpty()) {
        stopPushLoops();
      } else {
        startPushLoops();
      }
    }

    synchronized PushConfig getPushConfig() {
      return subscription.getPushConfig();
    }

    synchronized void setRetainAckedMessages(boolean updatedValue) {
      subscription = subscription.toBuilder().setRetainAckedMessages(updatedValue).build();
      backlog.setRetainAckedMessages(updatedValue);
    }

    synchronized void setAckDeadline(java.time.Duration updatedAckDeadline) throws StatusException {
      subscription =
          subscription.toBuilder()
              .setAckDeadlineSeconds((int) updatedAckDeadline.getSeconds())
              .build();
      backlog.setAckDeadline(updatedAckDeadline);
    }

    synchronized void setMessageRetentionDuration(Duration updatedRetentionDuration) {
      subscription =
          subscription.toBuilder().setMessageRetentionDuration(updatedRetentionDuration).build();
    }

    synchronized void markTopicDeleted() {
      if (!subscription.getTopic().equals(DELETED_TOPIC)) {
        subscription = Subscription.newBuilder(subscription).setTopic(DELETED_TOPIC).build();
      }
    }

    Backlog getBacklog() {
      return backlog;
    }

    private synchronized void startPushLoops() {
      stopPushLoops();
      for (int i = 0; i < NUM_CONCURRENT_PUSH_REQUESTS; ++i) {
        PushLoop pushLoop =
            new PushLoop(subscription, backlog, pusherRegistry, PUSH_POLLING_INTERVAL_MS);
        pushLoops.add(pushLoop);
        pushExecutor.execute(pushLoop);
      }
    }

    private synchronized void stopPushLoops() {
      for (PushLoop pushLoop : pushLoops) {
        pushLoop.stop();
      }
      pushLoops.clear();
    }
  }

  @Override
  public Policy setIamPolicy(SetIamPolicyRequest request) throws StatusException {
    ProjectAndResourceKey resourceKey =
        ProjectAndResourceKey.parseFrom(
            request.getResource(), TOPICS_COLLECTION_NAME, SUBSCRIPTIONS_COLLECTION_NAME);
    String policyName = getPolicyName(resourceKey);

    if (resourceKey.resourceName.equals(TOPICS_COLLECTION_NAME)) {
      authorizationHelper.requirePermissions(
          Context.current(), policyName, PubsubPermissions.TOPICS_SET_IAM_POLICY);
    } else {
      authorizationHelper.requirePermissions(
          Context.current(), policyName, PubsubPermissions.SUBSCRIPTIONS_SET_IAM_POLICY);
    }

    SetPolicyResponse response =
        iamInternal.setPolicy(
            SetPolicyRequest.newBuilder()
                .setPolicy(request.getPolicy())
                .setPolicyName(getPolicyName(resourceKey))
                .setParentName(getProjectPolicy(resourceKey))
                .build());

    return response.getPolicy();
  }

  @Override
  public Policy getIamPolicy(GetIamPolicyRequest request) throws StatusException {
    ProjectAndResourceKey resourceKey =
        ProjectAndResourceKey.parseFrom(
            request.getResource(), TOPICS_COLLECTION_NAME, SUBSCRIPTIONS_COLLECTION_NAME);
    String policyName = getPolicyName(resourceKey);

    if (resourceKey.resourceName.equals(TOPICS_COLLECTION_NAME)) {
      authorizationHelper.requirePermissions(
          Context.current(), policyName, PubsubPermissions.TOPICS_GET_IAM_POLICY);
    } else {
      authorizationHelper.requirePermissions(
          Context.current(), policyName, PubsubPermissions.SUBSCRIPTIONS_GET_IAM_POLICY);
    }

    GetPolicyResponse response =
        iamInternal.getPolicy(
            GetPolicyRequest.newBuilder().setPolicyName(getPolicyName(resourceKey)).build());

    return response.getPolicy();
  }

  @Override
  public TestIamPermissionsResponse testIamPermissions(TestIamPermissionsRequest request)
      throws StatusException {
    ProjectAndResourceKey resourceKey =
        ProjectAndResourceKey.parseFrom(
            request.getResource(), TOPICS_COLLECTION_NAME, SUBSCRIPTIONS_COLLECTION_NAME);
    TestPermissionsResponse response =
        iamInternal.testPermissions(
            TestPermissionsRequest.newBuilder()
                .setCredentials(authorizationHelper.getRequestCredentials(Context.current()))
                .setPolicyName(getPolicyName(resourceKey))
                .addAllPermissions(request.getPermissionsList())
                .build());

    return TestIamPermissionsResponse.newBuilder()
        .addAllPermissions(response.getPermissionsList())
        .build();
  }

  @VisibleForTesting
  SubscriptionData getSubscription(String name) {
    return subscriptions.get(name);
  }

  /**
   * Computes the policy name for a topic or subscription. Policy names have the following form:
   *
   * <ul>
   *   <li>pubsub/projects/PROJECT/topics/TOPIC
   *   <li>pubsub/projects/PROJECT/subscriptions/SUBSCRIPTION
   * </ul>
   */
  private static String getPolicyName(ProjectAndResourceKey resourceKey) {
    return "pubsub/" + resourceKey.getResourceUrl();
  }

  /**
   * Computes the policy name of the project that owns the given resource. This must be kept in sync
   * with the policy names used by the IAM fake. Currently, the names have the form
   * "resourcemanager/projects/PROJECT"
   */
  private static String getProjectPolicy(ProjectAndResourceKey resourceKey) {
    return getProjectPolicy("projects/" + resourceKey.project);
  }

  /**
   * Computes the policy name for the given project (see {@link
   * #getProjectPolicy(ProjectAndResourceKey)} for details). The project name must start with
   * "projects/".
   *
   * @throws IllegalArgumentException if the project name does not start with "projects/"
   */
  private static String getProjectPolicy(String projectName) {
    Preconditions.checkArgument(projectName.startsWith("projects/"));
    return "resourcemanager/" + projectName;
  }
}
