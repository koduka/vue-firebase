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

import com.google.pubsub.v1.DeleteTopicRequest;
import com.google.pubsub.v1.GetTopicRequest;
import com.google.pubsub.v1.ListTopicSnapshotsRequest;
import com.google.pubsub.v1.ListTopicSnapshotsResponse;
import com.google.pubsub.v1.ListTopicSubscriptionsRequest;
import com.google.pubsub.v1.ListTopicSubscriptionsResponse;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ListTopicsResponse;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.Topic;
import io.grpc.StatusException;

/** Abstraction of the v1 Publisher API. */
interface PublisherService {
  Topic createTopic(Topic topic) throws StatusException;

  PublishResponse publish(PublishRequest request) throws StatusException;

  Topic getTopic(GetTopicRequest request) throws StatusException;

  ListTopicsResponse listTopics(ListTopicsRequest request) throws StatusException;

  ListTopicSubscriptionsResponse listTopicSubscriptions(ListTopicSubscriptionsRequest request)
      throws StatusException;

  ListTopicSnapshotsResponse listTopicSnapshots(ListTopicSnapshotsRequest request)
      throws StatusException;

  void deleteTopic(DeleteTopicRequest request) throws StatusException;
}
