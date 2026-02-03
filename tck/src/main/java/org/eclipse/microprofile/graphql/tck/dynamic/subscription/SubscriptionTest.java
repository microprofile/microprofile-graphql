/*
 * Copyright 2020, 2025 Contributors to the Eclipse Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 *
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.eclipse.microprofile.graphql.tck.dynamic.subscription;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.eclipse.microprofile.graphql.tck.dynamic.DeployableUnit;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.RunAsClient;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.Assert;
import org.testng.annotations.Test;

import jakarta.json.Json;
import jakarta.json.JsonObject;

/**
 * Tests for GraphQL Subscriptions over WebSocket using the graphql-ws protocol.
 *
 * This test validates that implementations support WebSocket transport for subscriptions as mandated by the
 * MicroProfile GraphQL specification.
 *
 * @author Phillip Kruger (phillip.kruger@redhat.com)
 */
public class SubscriptionTest extends Arquillian {
    private static final Logger LOG = Logger.getLogger(SubscriptionTest.class.getName());
    private static final int TIMEOUT_SECONDS = 30;

    @ArquillianResource
    private URI uri;

    @Deployment
    public static Archive<?> getDeployment() throws Exception {
        return DeployableUnit.getDeployment("tck-subscription");
    }

    @RunAsClient
    @Test
    public void testBasicSubscription() throws Exception {
        LOG.info("Testing basic subscription (heroUpdates)");

        String subscriptionQuery = "subscription { heroUpdates { name realName } }";

        try (GraphQLWSClient client = new GraphQLWSClient(getWebSocketUri())) {
            client.connect();

            List<JsonObject> results = new ArrayList<>();
            CountDownLatch completeLatch = new CountDownLatch(1);

            client.subscribe(subscriptionQuery, null, new GraphQLWSClient.SubscriptionHandler() {
                @Override
                public void onNext(JsonObject data) {
                    results.add(data);
                }

                @Override
                public void onComplete() {
                    completeLatch.countDown();
                }

                @Override
                public void onError(JsonObject errors) {
                    Assert.fail("Subscription failed with errors: " + errors);
                }
            });

            // Wait for subscription to complete (heroUpdates emits 3 heroes)
            boolean completed = completeLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assert.assertTrue(completed, "Subscription did not complete within timeout");

            // Verify we received 3 hero updates
            Assert.assertEquals(results.size(), 3, "Expected 3 hero updates");

            // Verify each result has the expected structure
            for (JsonObject result : results) {
                Assert.assertTrue(result.containsKey("heroUpdates"), "Result should contain heroUpdates");
                JsonObject hero = result.getJsonObject("heroUpdates");
                Assert.assertTrue(hero.containsKey("name"), "Hero should have name field");
                Assert.assertTrue(hero.containsKey("realName"), "Hero should have realName field");
            }
        }
    }

    @RunAsClient
    @Test
    public void testSubscriptionWithArgument() throws Exception {
        LOG.info("Testing subscription with argument (heroByName)");

        String subscriptionQuery = "subscription($name: String) { heroByName(name: $name) { name realName } }";
        JsonObject variables = Json.createObjectBuilder()
                .add("name", "Iron Man")
                .build();

        try (GraphQLWSClient client = new GraphQLWSClient(getWebSocketUri())) {
            client.connect();

            List<JsonObject> results = new ArrayList<>();
            CountDownLatch completeLatch = new CountDownLatch(1);

            client.subscribe(subscriptionQuery, variables, new GraphQLWSClient.SubscriptionHandler() {
                @Override
                public void onNext(JsonObject data) {
                    results.add(data);
                }

                @Override
                public void onComplete() {
                    completeLatch.countDown();
                }

                @Override
                public void onError(JsonObject errors) {
                    Assert.fail("Subscription failed with errors: " + errors);
                }
            });

            // Wait for subscription to complete (heroByName emits 1 hero)
            boolean completed = completeLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assert.assertTrue(completed, "Subscription did not complete within timeout");

            // Verify we received 1 hero update
            Assert.assertEquals(results.size(), 1, "Expected 1 hero update");

            // Verify the result
            JsonObject result = results.get(0);
            Assert.assertTrue(result.containsKey("heroByName"), "Result should contain heroByName");
            JsonObject hero = result.getJsonObject("heroByName");
            Assert.assertEquals(hero.getString("name"), "Iron Man", "Hero name should be Iron Man");
        }
    }

    @RunAsClient
    @Test
    public void testSubscriptionWithDifferentType() throws Exception {
        LOG.info("Testing subscription with different return type (teamUpdates)");

        String subscriptionQuery = "subscription { teamUpdates { name } }";

        try (GraphQLWSClient client = new GraphQLWSClient(getWebSocketUri())) {
            client.connect();

            List<JsonObject> results = new ArrayList<>();
            CountDownLatch completeLatch = new CountDownLatch(1);

            client.subscribe(subscriptionQuery, null, new GraphQLWSClient.SubscriptionHandler() {
                @Override
                public void onNext(JsonObject data) {
                    results.add(data);
                }

                @Override
                public void onComplete() {
                    completeLatch.countDown();
                }

                @Override
                public void onError(JsonObject errors) {
                    Assert.fail("Subscription failed with errors: " + errors);
                }
            });

            // Wait for subscription to complete (teamUpdates emits 2 teams)
            boolean completed = completeLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assert.assertTrue(completed, "Subscription did not complete within timeout");

            // Verify we received 2 team updates
            Assert.assertEquals(results.size(), 2, "Expected 2 team updates");

            // Verify each result has the expected structure
            for (JsonObject result : results) {
                Assert.assertTrue(result.containsKey("teamUpdates"), "Result should contain teamUpdates");
                JsonObject team = result.getJsonObject("teamUpdates");
                Assert.assertTrue(team.containsKey("name"), "Team should have name field");
            }
        }
    }

    @RunAsClient
    @Test
    public void testSubscriptionCancellation() throws Exception {
        LOG.info("Testing subscription cancellation");

        String subscriptionQuery = "subscription { heroUpdates { name } }";

        try (GraphQLWSClient client = new GraphQLWSClient(getWebSocketUri())) {
            client.connect();

            List<JsonObject> results = new ArrayList<>();
            CountDownLatch firstItemLatch = new CountDownLatch(1);

            String operationId = client.subscribe(subscriptionQuery, null, new GraphQLWSClient.SubscriptionHandler() {
                @Override
                public void onNext(JsonObject data) {
                    results.add(data);
                    firstItemLatch.countDown();
                }

                @Override
                public void onComplete() {
                    // Should not complete before cancellation
                }

                @Override
                public void onError(JsonObject errors) {
                    Assert.fail("Subscription failed with errors: " + errors);
                }
            });

            // Wait for first item
            boolean receivedFirst = firstItemLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assert.assertTrue(receivedFirst, "Did not receive first item within timeout");

            // Cancel the subscription
            client.stop(operationId);

            // Wait a bit to ensure no more items arrive
            Thread.sleep(1000);

            // Verify we only received 1 item (subscription was cancelled)
            Assert.assertTrue(results.size() <= 2,
                    "Expected at most 2 items before cancellation, got " + results.size());
        }
    }

    private URI getWebSocketUri() {
        try {
            String httpUri = uri.toString();
            String wsUri = httpUri.replace("http://", "ws://").replace("https://", "wss://");
            if (!wsUri.endsWith("/")) {
                wsUri += "/";
            }
            wsUri += "graphql";
            return URI.create(wsUri);
        } catch (Exception e) {
            throw new RuntimeException("Failed to construct WebSocket URI", e);
        }
    }
}
