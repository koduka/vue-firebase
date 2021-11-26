package com.google.cloud.pubsub.testing.v1;

/** An interface for ordering key hasher. */
interface OrderingKeyHasher {
  int hashCode(String key, int numBuckets);
}
