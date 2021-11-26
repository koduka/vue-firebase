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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.ImmutableSet;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import javax.xml.bind.DatatypeConverter;
import org.json.simple.JSONObject;

/**
 * Pushes the message to the HTTP endpoint as JSON with a blocking call.
 */
public class HttpEndpointPusher implements EndpointPusher {
  // The documented set of HTTP response codes that indicate successful push acknowledgement.
  private static final ImmutableSet<Integer> ACK_RESPONSE_CODES =
      ImmutableSet.of(102, 200, 201, 203, 204);

  @Override
  public void validateEndpoint(PushConfig config) throws IllegalArgumentException {
    final String endpoint = config.getPushEndpoint();
    try {
      new URL(endpoint);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Endpoint is not a valid URL: " + endpoint, e);
    }
  }

  @Override
  public void push(PubsubMessage message, Subscription subscription) throws IOException {
    // For simplicity, we open and close a new connection with every request.
    final String content = toJsonStringForPush(message, subscription.getName());
    final String endpoint = subscription.getPushConfig().getPushEndpoint();
    final URL url = new URL(endpoint);
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setDoOutput(true);
    conn.setConnectTimeout(1000 * subscription.getAckDeadlineSeconds());
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setRequestProperty("Content-Length", Integer.toString(content.length()));
    try (OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream(), UTF_8)) {
      writer.write(content);
    } finally {
      conn.disconnect();
    }
    if (!ACK_RESPONSE_CODES.contains(conn.getResponseCode())) {
      throw new IOException(
          "Fetched url '"
              + endpoint
              + "' but got a response code that is not an acknowledgement: "
              + conn.getResponseCode());
    }
  }

  /**
   * Converts a PubsubMessage to its JSON push request equivalent and returns it.
   */
  // Silence warnings about references to JSONObject as HashMap needing to be parameterized.
  @SuppressWarnings("unchecked")
  private String toJsonStringForPush(PubsubMessage from, String subscriptionName) {
    // Follow the push request body example from sample code:
    //
    // {
    //   "message": {
    //     "attributes": {
    //       "string-value": "string-value",
    //       // ... more attributes
    //     },
    //     "data": "base64-no-line-feeds-variant-representation-of-payload",
    //     "messageId": "string-value"
    //   },
    //   "subscription": "string-value"
    // }
    //
    JSONObject attributes = new JSONObject();
    attributes.putAll(from.getAttributesMap());
    JSONObject message = new JSONObject();
    message.put("attributes", attributes);
    message.put("data", DatatypeConverter.printBase64Binary(from.getData().toByteArray()));
    message.put("messageId", from.getMessageId());

    JSONObject push = new JSONObject();
    push.put("message", message);
    push.put("subscription", subscriptionName);
    return push.toJSONString();
  }
}
