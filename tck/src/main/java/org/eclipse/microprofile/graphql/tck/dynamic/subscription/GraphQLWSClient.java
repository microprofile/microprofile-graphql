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

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonReader;

/**
 * A simple WebSocket client that implements the graphql-transport-ws protocol using Vert.x.
 *
 * This client is used for testing GraphQL subscriptions over WebSocket. It implements the graphql-transport-ws protocol
 * as specified in: https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md
 *
 * @author Phillip Kruger (phillip.kruger@redhat.com)
 */
public class GraphQLWSClient implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(GraphQLWSClient.class.getName());

    // Protocol message types
    private static final String MESSAGE_CONNECTION_INIT = "connection_init";
    private static final String MESSAGE_CONNECTION_ACK = "connection_ack";
    private static final String MESSAGE_PING = "ping";
    private static final String MESSAGE_PONG = "pong";
    private static final String MESSAGE_SUBSCRIBE = "subscribe";
    private static final String MESSAGE_NEXT = "next";
    private static final String MESSAGE_ERROR = "error";
    private static final String MESSAGE_COMPLETE = "complete";

    private final URI uri;
    private final Vertx vertx;
    private final HttpClient client;
    private WebSocket webSocket;
    private final AtomicInteger operationIdCounter = new AtomicInteger(0);
    private final Map<String, SubscriptionHandler> subscriptions = new ConcurrentHashMap<>();
    private final CountDownLatch connectionAckLatch = new CountDownLatch(1);

    public GraphQLWSClient(URI uri) {
        this.uri = uri;
        this.vertx = Vertx.vertx();
        HttpClientOptions options = new HttpClientOptions()
                .setDefaultHost(uri.getHost())
                .setDefaultPort(uri.getPort() > 0 ? uri.getPort() : 80);
        this.client = vertx.createHttpClient(options);
    }

    public void connect() throws Exception {
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch[] errorLatch = {null};

        WebSocketConnectOptions options = new WebSocketConnectOptions()
                .setHost(uri.getHost())
                .setPort(uri.getPort() > 0 ? uri.getPort() : 80)
                .setURI(uri.getPath())
                .addSubProtocol("graphql-transport-ws");

        client.webSocket(options, ar -> {
            if (ar.succeeded()) {
                webSocket = ar.result();
                setupWebSocketHandlers();
                connectLatch.countDown();

                // Send connection_init
                JsonObject initMessage = Json.createObjectBuilder()
                        .add("type", MESSAGE_CONNECTION_INIT)
                        .build();
                sendMessage(initMessage);
            } else {
                LOG.severe("Failed to connect WebSocket: " + ar.cause().getMessage());
                errorLatch[0] = new CountDownLatch(0);
                connectLatch.countDown();
            }
        });

        boolean connected = connectLatch.await(10, TimeUnit.SECONDS);
        if (!connected || errorLatch[0] != null) {
            throw new RuntimeException("Failed to connect WebSocket within timeout");
        }

        // Wait for connection_ack
        boolean acked = connectionAckLatch.await(10, TimeUnit.SECONDS);
        if (!acked) {
            throw new RuntimeException("Did not receive connection_ack within timeout");
        }
        LOG.info("WebSocket connection established and acknowledged");
    }

    private void setupWebSocketHandlers() {
        webSocket.textMessageHandler(message -> {
            LOG.fine("Received message: " + message);
            try (JsonReader reader = Json.createReader(new StringReader(message))) {
                JsonObject json = reader.readObject();
                String type = json.getString("type");

                switch (type) {
                    case MESSAGE_CONNECTION_ACK :
                        connectionAckLatch.countDown();
                        LOG.info("Connection acknowledged");
                        break;

                    case MESSAGE_PING :
                        // Respond with pong
                        JsonObject pong = Json.createObjectBuilder()
                                .add("type", MESSAGE_PONG)
                                .build();
                        sendMessage(pong);
                        break;

                    case MESSAGE_NEXT :
                        handleNext(json);
                        break;

                    case MESSAGE_ERROR :
                        handleError(json);
                        break;

                    case MESSAGE_COMPLETE :
                        handleComplete(json);
                        break;

                    default :
                        LOG.warning("Unknown message type: " + type);
                }
            } catch (Exception e) {
                LOG.severe("Error processing message: " + e.getMessage());
                e.printStackTrace();
            }
        });

        webSocket.exceptionHandler(throwable -> {
            LOG.severe("WebSocket error: " + throwable.getMessage());
            throwable.printStackTrace();
        });

        webSocket.closeHandler(v -> {
            LOG.info("WebSocket connection closed");
        });
    }

    public String subscribe(String query, JsonObject variables, SubscriptionHandler handler) {
        String operationId = String.valueOf(operationIdCounter.incrementAndGet());
        subscriptions.put(operationId, handler);

        JsonObjectBuilder payloadBuilder = Json.createObjectBuilder()
                .add("query", query);

        if (variables != null && !variables.isEmpty()) {
            payloadBuilder.add("variables", variables);
        }

        JsonObject message = Json.createObjectBuilder()
                .add("id", operationId)
                .add("type", MESSAGE_SUBSCRIBE)
                .add("payload", payloadBuilder.build())
                .build();

        sendMessage(message);
        LOG.info("Sent subscription with operationId: " + operationId);
        return operationId;
    }

    public void stop(String operationId) {
        JsonObject message = Json.createObjectBuilder()
                .add("id", operationId)
                .add("type", "complete")
                .build();

        sendMessage(message);
        subscriptions.remove(operationId);
        LOG.info("Stopped subscription: " + operationId);
    }

    @Override
    public void close() throws IOException {
        if (webSocket != null && !webSocket.isClosed()) {
            webSocket.close();
        }
        if (client != null) {
            client.close();
        }
        if (vertx != null) {
            vertx.close();
        }
    }

    private void handleNext(JsonObject message) {
        String id = message.getString("id");
        SubscriptionHandler handler = subscriptions.get(id);
        if (handler != null) {
            JsonObject payload = message.getJsonObject("payload");
            JsonObject data = payload.getJsonObject("data");
            handler.onNext(data);
        }
    }

    private void handleError(JsonObject message) {
        String id = message.getString("id");
        SubscriptionHandler handler = subscriptions.get(id);
        if (handler != null) {
            // The payload for errors in graphql-transport-ws is an array of error objects
            // For simplicity, we'll pass the whole message to the handler
            handler.onError(message);
            subscriptions.remove(id);
        }
    }

    private void handleComplete(JsonObject message) {
        String id = message.getString("id");
        SubscriptionHandler handler = subscriptions.get(id);
        if (handler != null) {
            handler.onComplete();
            subscriptions.remove(id);
        }
    }

    private void sendMessage(JsonObject message) {
        if (webSocket != null && !webSocket.isClosed()) {
            webSocket.writeTextMessage(message.toString());
        } else {
            LOG.severe("WebSocket is not open, cannot send message");
        }
    }

    /**
     * Handler interface for subscription events
     */
    public interface SubscriptionHandler {
        void onNext(JsonObject data);

        void onComplete();

        void onError(JsonObject errors);
    }
}
