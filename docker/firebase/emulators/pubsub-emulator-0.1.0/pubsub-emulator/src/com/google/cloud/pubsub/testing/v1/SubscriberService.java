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

import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.CreateSnapshotRequest;
import com.google.pubsub.v1.DeleteSnapshotRequest;
import com.google.pubsub.v1.DeleteSubscriptionRequest;
import com.google.pubsub.v1.GetSnapshotRequest;
import com.google.pubsub.v1.GetSubscriptionRequest;
import com.google.pubsub.v1.ListSnapshotsRequest;
import com.google.pubsub.v1.ListSnapshotsResponse;
import com.google.pubsub.v1.ListSubscriptionsRequest;
import com.google.pubsub.v1.ListSubscriptionsResponse;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.ModifyPushConfigRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.SeekRequest;
import com.google.pubsub.v1.SeekResponse;
import com.google.pubsub.v1.Snapshot;
import com.google.pubsub.v1.StreamingPullRequest;
import com.google.pubsub.v1.StreamingPullResponse;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.UpdateSubscriptionRequest;
import io.gapi.emulators.util.Observer;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

/** Abstraction of the v1 Subscriber API. */
interface SubscriberService {
  Subscription createSubscription(Subscription subscription) throws StatusException;

  Subscription getSubscription(GetSubscriptionRequest request) throws StatusException;

  ListSubscriptionsResponse listSubscriptions(ListSubscriptionsRequest request)
      throws StatusException;

  void deleteSubscription(DeleteSubscriptionRequest request) throws StatusException;

  Subscription updateSubscription(UpdateSubscriptionRequest request) throws StatusException;

  Snapshot createSnapshot(CreateSnapshotRequest request) throws StatusException;

  Snapshot getSnapshot(GetSnapshotRequest request) throws StatusException;

  ListSnapshotsResponse listSnapshots(ListSnapshotsRequest request) throws StatusException;

  void deleteSnapshot(DeleteSnapshotRequest request) throws StatusException;

  SeekResponse seek(SeekRequest request) throws StatusException;

  void modifyAckDeadline(ModifyAckDeadlineRequest request) throws StatusException;

  void acknowledge(AcknowledgeRequest request) throws StatusException;

  // Takes a deadline object to interface with different RPC implementations.
  void pull(PullRequest request, Observer<PullResponse> observer, RpcDeadline deadline);

  /**
   * @param responseObserver the observer that the streaming pull implementation will call for each
   *     new response value. If onError() is called on this observer, the throwable is likely to be
   *     a StatusException.
   * @param deadline the stream deadline
   * @return the observer from which the streaming pull implementation will read new request values
   */
  StreamObserver<StreamingPullRequest> streamingPull(
      StreamObserver<StreamingPullResponse> responseObserver, RpcDeadline deadline);

  void modifyPushConfig(ModifyPushConfigRequest request) throws StatusException;
}
