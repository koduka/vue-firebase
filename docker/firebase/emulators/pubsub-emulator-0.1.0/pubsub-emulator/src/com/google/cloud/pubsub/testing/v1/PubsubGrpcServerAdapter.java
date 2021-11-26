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

import com.google.cloud.iam.testing.v1.shared.authorization.AuthInterceptor;
import com.google.iam.v1.GetIamPolicyRequest;
import com.google.iam.v1.IAMPolicyGrpc;
import com.google.iam.v1.IAMPolicyGrpc.IAMPolicyImplBase;
import com.google.iam.v1.IamPolicyProto;
import com.google.iam.v1.Policy;
import com.google.iam.v1.SetIamPolicyRequest;
import com.google.iam.v1.TestIamPermissionsRequest;
import com.google.iam.v1.TestIamPermissionsResponse;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.Empty;
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
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PublisherGrpc.PublisherImplBase;
import com.google.pubsub.v1.PubsubProto;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.SeekRequest;
import com.google.pubsub.v1.SeekResponse;
import com.google.pubsub.v1.Snapshot;
import com.google.pubsub.v1.StreamingPullRequest;
import com.google.pubsub.v1.StreamingPullResponse;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberImplBase;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.UpdateSubscriptionRequest;
import io.gapi.emulators.grpc.GrpcServer;
import io.gapi.emulators.grpc.HttpJsonAdapter;
import io.gapi.emulators.grpc.StreamObserverUtil;
import io.gapi.emulators.protobuf.HttpConfig;
import io.gapi.emulators.protobuf.HttpConfig.HttpKind;
import com.google.api.pathtemplate.PathTemplate;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** A gRPC interface wrapper for Publisher and Subscriber delegates. */
public class PubsubGrpcServerAdapter {
  private final PublisherService publisher;
  private final SubscriberService subscriber;
  private final IamPolicyService iamService;
  private long defaultPullDeadlineMillis;

  private final PublisherImplBase publisherImpl =
      new PublisherImplBase() {
        @Override
        public void createTopic(Topic request, StreamObserver<Topic> responseObserver) {
          try {
            responseObserver.onNext(publisher.createTopic(request));
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void publish(
            PublishRequest request, StreamObserver<PublishResponse> responseObserver) {
          try {
            responseObserver.onNext(publisher.publish(request));
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void getTopic(GetTopicRequest request, StreamObserver<Topic> responseObserver) {
          try {
            responseObserver.onNext(publisher.getTopic(request));
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void listTopics(
            ListTopicsRequest request, StreamObserver<ListTopicsResponse> responseObserver) {
          try {
            responseObserver.onNext(publisher.listTopics(request));
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void listTopicSubscriptions(
            ListTopicSubscriptionsRequest request,
            StreamObserver<ListTopicSubscriptionsResponse> responseObserver) {
          try {
            responseObserver.onNext(publisher.listTopicSubscriptions(request));
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void listTopicSnapshots(
            ListTopicSnapshotsRequest request,
            StreamObserver<ListTopicSnapshotsResponse> responseObserver) {
          try {
            responseObserver.onNext(publisher.listTopicSnapshots(request));
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void deleteTopic(
            DeleteTopicRequest request, StreamObserver<Empty> responseObserver) {
          try {
            publisher.deleteTopic(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }
      };

  private final SubscriberImplBase subscriberImpl =
      new SubscriberImplBase() {
        @Override
        public void createSubscription(
            Subscription request, StreamObserver<Subscription> responseObserver) {
          try {
            responseObserver.onNext(subscriber.createSubscription(request));
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void getSubscription(
            GetSubscriptionRequest request, StreamObserver<Subscription> responseObserver) {
          try {
            responseObserver.onNext(subscriber.getSubscription(request));
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void listSubscriptions(
            ListSubscriptionsRequest request,
            StreamObserver<ListSubscriptionsResponse> responseObserver) {
          try {
            responseObserver.onNext(subscriber.listSubscriptions(request));
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void deleteSubscription(
            DeleteSubscriptionRequest request, StreamObserver<Empty> responseObserver) {
          try {
            subscriber.deleteSubscription(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void updateSubscription(
            UpdateSubscriptionRequest request, StreamObserver<Subscription> responseObserver) {
          try {
            responseObserver.onNext(subscriber.updateSubscription(request));
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void createSnapshot(
            CreateSnapshotRequest request, StreamObserver<Snapshot> responseObserver) {
          try {
            responseObserver.onNext(subscriber.createSnapshot(request));
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void getSnapshot(
            GetSnapshotRequest request, StreamObserver<Snapshot> responseObserver) {
          try {
            responseObserver.onNext(subscriber.getSnapshot(request));
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void listSnapshots(
            ListSnapshotsRequest request, StreamObserver<ListSnapshotsResponse> responseObserver) {
          try {
            responseObserver.onNext(subscriber.listSnapshots(request));
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void deleteSnapshot(
            DeleteSnapshotRequest request, StreamObserver<Empty> responseObserver) {
          try {
            subscriber.deleteSnapshot(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void seek(SeekRequest request, StreamObserver<SeekResponse> responseObserver) {
          try {
            responseObserver.onNext(subscriber.seek(request));
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void modifyAckDeadline(
            ModifyAckDeadlineRequest request, StreamObserver<Empty> responseObserver) {
          try {
            subscriber.modifyAckDeadline(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void acknowledge(
            AcknowledgeRequest request, StreamObserver<Empty> responseObserver) {
          try {
            subscriber.acknowledge(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void pull(PullRequest request, final StreamObserver<PullResponse> responseObserver) {
          final Deadline ctxDeadline = Context.current().getDeadline();
          subscriber.pull(
              request,
              StreamObserverUtil.asObserver(responseObserver),
              new RpcDeadline(
                  ctxDeadline != null
                      ? ctxDeadline.timeRemaining(TimeUnit.MILLISECONDS)
                      : defaultPullDeadlineMillis));
        }

        @Override
        public StreamObserver<StreamingPullRequest> streamingPull(
            StreamObserver<StreamingPullResponse> responseObserver) {
          final Deadline ctxDeadline = Context.current().getDeadline();
          return subscriber.streamingPull(
              responseObserver,
              // Default to INFINITE, since streamingPull is fundamentally long running.
              new RpcDeadline(
                  ctxDeadline != null
                      ? ctxDeadline.timeRemaining(TimeUnit.MILLISECONDS)
                      : RpcDeadline.INFINITE));
        }

        @Override
        public void modifyPushConfig(
            ModifyPushConfigRequest request, StreamObserver<Empty> responseObserver) {
          try {
            subscriber.modifyPushConfig(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }
      };

  private final IAMPolicyImplBase iamPolicyImpl =
      new IAMPolicyImplBase() {
        @Override
        public void setIamPolicy(
            SetIamPolicyRequest request, StreamObserver<Policy> responseObserver) {
          try {
            responseObserver.onNext(iamService.setIamPolicy(request));
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void getIamPolicy(
            GetIamPolicyRequest request, StreamObserver<Policy> responseObserver) {
          try {
            responseObserver.onNext(iamService.getIamPolicy(request));
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }

        @Override
        public void testIamPermissions(
            TestIamPermissionsRequest request,
            StreamObserver<TestIamPermissionsResponse> responseObserver) {
          try {
            responseObserver.onNext(iamService.testIamPermissions(request));
            responseObserver.onCompleted();
          } catch (StatusException e) {
            responseObserver.onError(e);
          }
        }
      };

  public static GrpcServer build(String host, int port, PubsubGrpcServerAdapter serverAdapter) {
    // Intercept calls to each service to extract the access token from the headers.
    final AuthInterceptor authInterceptor = new AuthInterceptor();
    final List<ServerServiceDefinition> services =
        Arrays.asList(
            ServerInterceptors.intercept(serverAdapter.publisherImpl, authInterceptor),
            ServerInterceptors.intercept(serverAdapter.subscriberImpl, authInterceptor),
            ServerInterceptors.intercept(serverAdapter.iamPolicyImpl, authInterceptor));

    // The Json adapter actually performs gRPC calls to the server itself.
    final ManagedChannel channel =
        ManagedChannelBuilder.forAddress("localhost", port).usePlaintext(true).build();

    final GrpcServer server =
        new GrpcServer(host, port, services) {
          @Override
          public void stop() {
            super.stop();
            channel.shutdownNow();
          }
        };

    final HttpJsonAdapter jsonAdapter = new HttpJsonAdapter();

    setUpIamPolicyJsonApi(channel, jsonAdapter);
    jsonAdapter.addService(
        PubsubProto.getDescriptor().findServiceByName("Publisher"), PublisherGrpc.newStub(channel));
    jsonAdapter.addService(
        PubsubProto.getDescriptor().findServiceByName("Subscriber"),
        SubscriberGrpc.newStub(channel));

    server.addHttpHandler(jsonAdapter);
    return server;
  }

  /**
   * Adds the URL mappings for the JSON API of the IAM Policy service to the given {@link
   * HttpJsonAdapter}. The JSON API will make gRPC calls to the given {@link ManagedChannel}.
   */
  private static void setUpIamPolicyJsonApi(ManagedChannel channel, HttpJsonAdapter jsonAdapter)
      throws AssertionError {
    ServiceDescriptor iamPolicyDescriptor =
        IamPolicyProto.getDescriptor().findServiceByName("IAMPolicy");
    IAMPolicyGrpc.IAMPolicyStub iamPolicyStub = IAMPolicyGrpc.newStub(channel);

    try {
      // We need to add these mappings explicitly because the IAMPolicy proto does not include (and
      // can't include, because the URLs are different for each cloud service) the HTTP annotations
      // used by HttpJsonAdapter.addService().
      jsonAdapter.addMethodHandler(
          jsonAdapter
          .new UnaryMethodHandler(
              HttpConfig.create(
                  HttpKind.POST,
                  PathTemplate.create("v1/{resource=projects/*/topics/*}:setIamPolicy"),
                  "*"),
              iamPolicyDescriptor.findMethodByName("SetIamPolicy"),
              iamPolicyStub));
      jsonAdapter.addMethodHandler(
          jsonAdapter
          .new UnaryMethodHandler(
              HttpConfig.create(
                  HttpKind.GET,
                  PathTemplate.create("v1/{resource=projects/*/topics/*}:getIamPolicy"),
                  ""),
              iamPolicyDescriptor.findMethodByName("GetIamPolicy"),
              iamPolicyStub));
      jsonAdapter.addMethodHandler(
          jsonAdapter
          .new UnaryMethodHandler(
              HttpConfig.create(
                  HttpKind.POST,
                  PathTemplate.create("v1/{resource=projects/*/topics/*}:testIamPermissions"),
                  "*"),
              iamPolicyDescriptor.findMethodByName("TestIamPermissions"),
              iamPolicyStub));

      jsonAdapter.addMethodHandler(
          jsonAdapter
          .new UnaryMethodHandler(
              HttpConfig.create(
                  HttpKind.POST,
                  PathTemplate.create("v1/{resource=projects/*/subscriptions/*}:setIamPolicy"),
                  "*"),
              iamPolicyDescriptor.findMethodByName("SetIamPolicy"),
              iamPolicyStub));
      jsonAdapter.addMethodHandler(
          jsonAdapter
          .new UnaryMethodHandler(
              HttpConfig.create(
                  HttpKind.GET,
                  PathTemplate.create("v1/{resource=projects/*/subscriptions/*}:getIamPolicy"),
                  ""),
              iamPolicyDescriptor.findMethodByName("GetIamPolicy"),
              iamPolicyStub));
      jsonAdapter.addMethodHandler(
          jsonAdapter
          .new UnaryMethodHandler(
              HttpConfig.create(
                  HttpKind.POST,
                  PathTemplate.create(
                      "v1/{resource=projects/*/subscriptions/*}:testIamPermissions"),
                  "*"),
              iamPolicyDescriptor.findMethodByName("TestIamPermissions"),
              iamPolicyStub));
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }

  public PubsubGrpcServerAdapter(
      PublisherService publisher, SubscriberService subscriber, IamPolicyService iamService) {
    this.publisher = publisher;
    this.subscriber = subscriber;
    this.iamService = iamService;
    this.defaultPullDeadlineMillis = RpcDeadline.DEFAULT_PULL_DEADLINE_MS;
  }

  void setDefaultPullDeadlineMillis(long deadlineMillis) {
    defaultPullDeadlineMillis = deadlineMillis;
  }
}
