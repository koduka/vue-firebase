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

import com.google.common.math.LongMath;

/**
 * Simple representation of an RPC deadline.
 */
public class RpcDeadline {
  public static final long INFINITE = Long.MAX_VALUE;

  public static final long DEFAULT_PULL_DEADLINE_MS = 90_000;

  private final long expireTimeMillis;

  public RpcDeadline(long durationMillis) {
    expireTimeMillis = LongMath.saturatedAdd(System.currentTimeMillis(), durationMillis);
  }

  public boolean isExpired() {
    return System.currentTimeMillis() > expireTimeMillis;
  }

  public long getRemainingMillis() {
    return expireTimeMillis - System.currentTimeMillis();
  }
}
