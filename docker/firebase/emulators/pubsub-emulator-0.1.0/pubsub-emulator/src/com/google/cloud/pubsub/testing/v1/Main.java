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

import com.google.cloud.iam.testing.v1.shared.authorization.AuthorizationHelper;
import com.google.cloud.iam.testing.v1.shared.authorization.IamAuthorizationHelper;
import com.google.cloud.iam.testing.v1.shared.authorization.NoopAuthorizationHelper;
import com.google.cloud.iam.testing.v1.shared.internalapi.GrpcIamInternalService;
import com.google.cloud.iam.testing.v1.shared.internalapi.IamInternalService;
import com.google.cloud.iam.testing.v1.shared.internalapi.UnimplementedIamInternalService;
import com.google.cloud.iam.testing.v1.shared.internalapi.protos.IamInternalGrpc;
import io.gapi.emulators.grpc.GrpcServer;
import io.gapi.emulators.netty.OkHandler;
import io.gapi.emulators.netty.OptionsHandler;
import io.gapi.emulators.netty.ResetHandler;
import io.gapi.emulators.netty.ShutdownHandler;
import io.gapi.emulators.netty.UnsupportedApiVersionHandler;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * Entrypoint to Pubsub v1 fake.
 */
public class Main {
  private static final Logger logger = Logger.getLogger(Main.class.getName());

  /**
   * Supports basic command line flag parsing.
   */
  private static class Flags {
    private Map<String, String> flags;
    private List<String> args;

    public Flags(String[] args) {
      flags = new HashMap<String, String>();
      this.args = new ArrayList<String>();
      for (String arg : args) {
        if (!addFlag(arg)) {
          this.args.add(arg);
        }
      }
    }

    public String getString(String flagName, String defaultValue) {
      if (flags.containsKey(flagName)) {
        return flags.get(flagName);
      }
      return defaultValue;
    }

    /**
     * Splits the flag value for flagName using splitRegex as the delimiter, and removes empty
     * strings from the post-split values.
     */
    public List<String> getStringsIgnoreEmpty(String flagName, String splitRegex) {
      List<String> results = new ArrayList<>();
      for (String value : getString(flagName, "").split(splitRegex)) {
        if (!value.isEmpty()) {
          results.add(value);
        }
      }
      return results;
    }

    public int getInt(String flagName, int defaultValue) {
      if (flags.containsKey(flagName)) {
        return Integer.parseInt(flags.get(flagName));
      }
      return defaultValue;
    }

    public long getLong(String flagName, long defaultValue) {
      if (flags.containsKey(flagName)) {
        return Long.parseLong(flags.get(flagName));
      }
      return defaultValue;
    }

    public boolean getBool(String flagName, boolean defaultValue) {
      if (flags.containsKey(flagName)) {
        return !flags.get(flagName).equals("false");
      }
      return defaultValue;
    }

    public List<String> getArgs() {
      return args;
    }

    private boolean addFlag(String arg) {
      if (arg.length() < 2 || arg.charAt(0) != '-') {
        return false;
      }
      String flag;
      if (arg.charAt(1) != '-') {
        flag = arg.substring(1);
      } else {
        if (arg.length() < 3 || arg.charAt(2) == '-') {
          return false;
        }
        flag = arg.substring(2);
      }
      String[] nameValue = flag.split("=", 2);
      flags.put(nameValue[0], nameValue.length > 1 ? nameValue[1] : "");
      return true;
    }
  }

  private static void printHelp() {
    System.out.println("Fake for Google Pub/Sub");
    System.out.println("--help: Show this help message and exit.");
    System.out.println("--host: The host the server binds as (defaults to localhost).");
    System.out.println("--port: The port on which the server should run.");
    System.out.println("--iam_host: The host the IAM emulator is bound to. Defaults to localhost.");
    System.out.println("--iam_port: The port on which the IAM emulator is listening for requests. "
        + "Defaults to 8090.");
    System.out.println("--projects: A comma-separated list of all valid project names. "
        + "If unspecified, all projects are valid.");
    // We intentionally do not document --pull_deadline, which we may remove in the future.
  }

  private static void printInfo() {
    System.out.println("This is the Google Pub/Sub fake.");
    System.out.println("Implementation may be incomplete or differ from the real system.");
  }

  /**
   * Flags:
   *
   * <ul>
   *   <li>--help: Show the help message and exit.
   *   <li>--host: The host the server binds as (defaults to localhost, not 0.0.0.0).
   *   <li>--port: The port on which the server should run.
   *   <li>--enable_iam_integration: If false, IAMPolicy methods and ACL checks will be disabled. In
   *       this case, the IAM emulator is not required. Defaults to false.
   *   <li>--iam_host: The host the IAM emulator is bound to. Defaults to localhost.
   *   <li>--iam_port: The port on which the IAM emulator is listening for requests. Defaults to
   *       8090.
   *   <li>--projects: A comma-separated list of all valid project names. If unspecified, all
   *       projects are valid.
   *   <li>--pull_deadline: The default RPC deadline to simulate for pull operations, in
   *       milliseconds. May be removed in the future.
   * </ul>
   */
  public static void main(String[] args) throws Exception {
    final Flags flags = new Flags(args);
    if (flags.getBool("help", false)) {
      printHelp();
      return;
    }

    printInfo();

    final Set<String> validProjects = new HashSet<>();
    validProjects.addAll(flags.getStringsIgnoreEmpty("projects", ","));

    IamInternalService iamInternalClient;
    AuthorizationHelper authorizationHelper;

    if (flags.getBool("enable_iam_integration", false)) {
      String iamHost = flags.getString("iam_host", "localhost");
      int iamPort = flags.getInt("iam_port", 8090);

      logger.info(String.format("IAM emulator is: %s:%d", iamHost, iamPort));

      iamInternalClient = new GrpcIamInternalService(IamInternalGrpc.newBlockingStub(
          ManagedChannelBuilder.forAddress(iamHost, iamPort).usePlaintext(true).build()));

      authorizationHelper = new IamAuthorizationHelper(iamInternalClient);
    } else {
      logger.info(
          "IAM integration is disabled. IAM policy methods and ACL checks are not supported");

      iamInternalClient = new UnimplementedIamInternalService();
      authorizationHelper = new NoopAuthorizationHelper();
    }

    final FakePubsubServer fakeServer =
        new FakePubsubServer(validProjects, iamInternalClient, authorizationHelper);
    final String fakeHost = flags.getString("host", null);
    final int fakePort = flags.getInt("port", 8080);

    final PubsubGrpcServerAdapter fakeAdapter = new PubsubGrpcServerAdapter(fakeServer, fakeServer,
        fakeServer);
    fakeAdapter.setDefaultPullDeadlineMillis(
        flags.getLong("pull_deadline", RpcDeadline.DEFAULT_PULL_DEADLINE_MS));
    final GrpcServer server = PubsubGrpcServerAdapter.build(fakeHost, fakePort, fakeAdapter);

    final ExecutorService executor = Executors.newScheduledThreadPool(2);
    final UnsupportedApiVersionHandler apiVersionHandler = new UnsupportedApiVersionHandler();
    apiVersionHandler.addSupportedVersion("v1");

    // Extra handlers.
    // Note: Handlers other than the root URI handler may change or be removed in the future!
    server.addHttpHandler(new OkHandler("/"));
    server.addHttpHandler(new OptionsHandler());
    server.addHttpHandler(new ResetHandler("/reset$", fakeServer));
    server.addHttpHandler(new ShutdownHandler("/shutdown$", server, executor));
    server.addHttpHandler(apiVersionHandler);

    server.start();
    logger.info("Server started, listening on " + fakePort);
    server.awaitTerminated();
    logger.info("Server terminated.");

    executor.shutdownNow();
  }
}
