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

import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;

import java.io.IOException;

/**
 * Pushes a message to an endpoint.
 */
public interface EndpointPusher {
  /**
   * Checks that the push endpoint associated with the given config is valid for this pusher.
   * 
   * @throws IllegalArgumentException If the endpoint is invalid for this pusher.
   */
  void validateEndpoint(PushConfig config) throws IllegalArgumentException;
  
  /**
   * Pushes a message to the push endpoint associated with a subscription.
   *
   * @param message the message to push.
   * @param subscription the subscription whose push endpoint to push to.
   * @throws IOException pushing failed.
   */
  void push(PubsubMessage message, Subscription subscription) throws IOException;
}
