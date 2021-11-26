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

import com.google.iam.v1.GetIamPolicyRequest;
import com.google.iam.v1.Policy;
import com.google.iam.v1.SetIamPolicyRequest;
import com.google.iam.v1.TestIamPermissionsRequest;
import com.google.iam.v1.TestIamPermissionsResponse;

import io.grpc.StatusException;

/**
 * Abstraction of IAMPolicy service. 
 * See https://github.com/googleapis/googleapis/blob/master/google/iam/v1/iam_policy.proto
 */
public interface IamPolicyService {
  Policy setIamPolicy(SetIamPolicyRequest request) throws StatusException;

  Policy getIamPolicy(GetIamPolicyRequest request) throws StatusException;

  TestIamPermissionsResponse testIamPermissions(TestIamPermissionsRequest request)
      throws StatusException;
}

