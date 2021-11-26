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

import com.google.common.base.Preconditions;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * A registry of EndpointPusher instances. Thread-safe.
 */
class EndpointPusherRegistry {
  // Maps endpoint prefixes to pushers for those endpoints. Entries added earlier take precedence
  // over entries added later.
  private final LinkedHashMap<Pattern, EndpointPusher> pushers;

  EndpointPusherRegistry() {
    this.pushers = new LinkedHashMap<>();
  }

  /**
   * Registers a pusher to be associated with a regular expression for matching endpoint values.
   * Entries added earlier take precedence over entries added later.
   */
  synchronized void register(Pattern endpointPattern, EndpointPusher pusher) {
    Preconditions.checkNotNull(endpointPattern);
    Preconditions.checkNotNull(pusher);
    pushers.put(endpointPattern, pusher);
  }

  /**
   * @param endpoint
   * @return the pusher that has been registered with a matching endpoint prefix. If no such pusher
   * has been registered, returns null.
   */
  @Nullable
  synchronized EndpointPusher getPusher(String endpoint) {
    for (Map.Entry<Pattern, EndpointPusher> entry : pushers.entrySet()) {
      if (entry.getKey().matcher(endpoint).find()) {
        return entry.getValue();
      }
    }
    return null;
  }
}
