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

import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Subscription;
import io.grpc.StatusException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/** An execution context that performs push delivery for a given subscription. Thread-safe. */
class PushLoop implements Runnable {
  private static final Logger logger = Logger.getLogger(PushLoop.class.getName());

  // The delay between failed push attempts.
  static final long PUSH_RETRY_AFTER_FAIL_DELAY_MS = 1000;

  private final Subscription subscription;
  private final Backlog backlog;
  private final EndpointPusherRegistry pusherRegistry;
  private final int checkIntervalMillis;
  private final AtomicBoolean running;
  private final AtomicBoolean stopRequested;

  /**
   * @param subscription the subscription whose message backlog to push
   * @param backlog the backlog of messages to push
   * @param pusherRegistry the registry of pusher implementations
   * @param checkIntervalMillis how often to check for new messages to push when the push queue is
   *     empty or a push response indicated failure
   */
  PushLoop(
      Subscription subscription,
      Backlog backlog,
      EndpointPusherRegistry pusherRegistry,
      int checkIntervalMillis) {
    this.subscription = subscription;
    this.backlog = backlog;
    this.pusherRegistry = pusherRegistry;
    this.checkIntervalMillis = checkIntervalMillis;
    this.running = new AtomicBoolean(false);
    this.stopRequested = new AtomicBoolean(false);
  }

  /**
   * Pushes backlogged messages to the endpoint if one is configured. Runs until stop() is called.
   */
  @Override
  public void run() {
    running.set(true);
    while (!stopRequested.get()) {
      if (dispatchMessages()) {
        continue;
      }
      // No fancy slow-start or backoff logic for now.
      try {
        Thread.sleep(checkIntervalMillis);
      } catch (InterruptedException e) {
        // Do nothing.
        Thread.currentThread().interrupt();
      }
    }
    running.set(false);
    stopRequested.set(false);
  }

  boolean isRunning() {
    return running.get();
  }

  /**
   * Causes run() to return if it is running, and turns any subsequent calls to run() into no-ops.
   * Does not block.
   */
  void stop() {
    stopRequested.set(true);
  }

  /**
   * Dispatches backlogged messages to the subscription's push endpoint.
   *
   * @return true iff any messages were pushed from the backlog
   */
  @VisibleForTesting
  boolean dispatchMessages() {
    final String endpoint = subscription.getPushConfig().getPushEndpoint();

    // Check for empty in case there is a race condition modifying the endpoint.
    if (!endpoint.isEmpty()) {
      List<ReceivedMessage> newMessages = backlog.pull(1);
      if (!newMessages.isEmpty()) {
        ReceivedMessage newMessage = newMessages.get(0);
        List<String> ackIds = Arrays.asList(newMessage.getAckId());
        try {
          if (push(newMessage.getMessage(), endpoint)) {
            backlog.acknowledge(ackIds);
            return true;
          } else {
            backlog.modifyAckDeadline(
                ackIds, Collections.nCopies(ackIds.size(), PUSH_RETRY_AFTER_FAIL_DELAY_MS));
          }
        } catch (StatusException e) {
          logger.log(Level.SEVERE, "Unexpected exception: " + e.getMessage(), e);
        }
      }
    }

    return false;
  }

  @VisibleForTesting
  boolean push(PubsubMessage message, String endpoint) {
    try {
      pusherRegistry.getPusher(endpoint).push(message, subscription);
    } catch (IOException e) {
      logger.log(Level.INFO, "Failed to push to endpoint '" + endpoint + "': " + e.getMessage(), e);
      return false;
    }
    return true;
  }
}
