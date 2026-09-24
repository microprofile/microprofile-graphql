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
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonReader;

/**
 * A simple WebSocket client that implements the graphql-transport-ws protocol using java.net.http.
 *
 * This client is used for testing GraphQL subscriptions over WebSocket. It implements the graphql-transport-ws protocol
 * as specified in: https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md
 *
 * @author Phillip Kruger (phillip.kruger@redhat.com)
 */
public class GraphQLWSClient implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(GraphQLWSClient.class.getName());
    private static final int SEND_TIMEOUT_SECONDS = 10;

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
    private final HttpClient httpClient;
    private WebSocket webSocket;
    private final AtomicInteger operationIdCounter = new AtomicInteger(0);
    private final Map<String, SubscriptionHandler> subscriptions = new ConcurrentHashMap<>();
    private final CountDownLatch connectionAckLatch = new CountDownLatch(1);

    public GraphQLWSClient(URI uri) {
        this.uri = uri;
        this.httpClient = HttpClient.newHttpClient();
    }

    public void connect() throws Exception {
        CountDownLatch connectLatch = new CountDownLatch(1);
        Exception[] connectError = {null};

        httpClient.newWebSocketBuilder()
                .subprotocols("graphql-transport-ws")
                .buildAsync(uri, new WebSocket.Listener() {
                    private final StringBuilder messageBuffer = new StringBuilder();

                    @Override
                    public void onOpen(WebSocket ws) {
                        webSocket = ws;
                        connectLatch.countDown();
                        ws.request(1);

                        JsonObject initMessage = Json.createObjectBuilder()
                                .add("type", MESSAGE_CONNECTION_INIT)
                                .build();
                        sendMessage(initMessage);
                    }

                    @Override
                    public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
                        messageBuffer.append(data);
                        if (last) {
                            String message = messageBuffer.toString();
                            messageBuffer.setLength(0);
                            handleMessage(message);
                        }
                        ws.request(1);
                        return null;
                    }

                    @Override
                    public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
                        LOG.info("WebSocket connection closed");
                        return null;
                    }

                    @Override
                    public void onError(WebSocket ws, Throwable error) {
                        LOG.severe("WebSocket error: " + error.getMessage());
                        error.printStackTrace();
                    }
                }).exceptionally(ex -> {
                    connectError[0] = new RuntimeException("Failed to connect WebSocket: " + ex.getMessage(), ex);
                    connectLatch.countDown();
                    return null;
                });

        boolean connected = connectLatch.await(10, TimeUnit.SECONDS);
        if (!connected) {
            throw new RuntimeException("Failed to connect WebSocket within timeout");
        }
        if (connectError[0] != null) {
            throw connectError[0];
        }

        boolean acked = connectionAckLatch.await(10, TimeUnit.SECONDS);
        if (!acked) {
            throw new RuntimeException("Did not receive connection_ack within timeout");
        }
        LOG.info("WebSocket connection established and acknowledged");
    }

    private void handleMessage(String message) {
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
    public synchronized void close() throws IOException {
        if (webSocket != null && !webSocket.isInputClosed() && !webSocket.isOutputClosed()) {
            try {
                await(webSocket.sendClose(WebSocket.NORMAL_CLOSURE, ""), "close");
            } catch (RuntimeException e) {
                // The server may already be closing the connection; that must not fail the test
                LOG.warning(e.getMessage());
            }
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

    /**
     * java.net.http.WebSocket allows only one outstanding send, and a send started while another is pending fails
     * through its returned future rather than by throwing. Sends are therefore serialized and awaited, so a message is
     * never silently dropped.
     */
    private synchronized void sendMessage(JsonObject message) {
        if (webSocket != null && !webSocket.isOutputClosed()) {
            await(webSocket.sendText(message.toString(), true), "send " + message.getString("type"));
        } else {
            LOG.severe("WebSocket is not open, cannot send message");
        }
    }

    private static void await(CompletableFuture<?> future, String action) {
        try {
            future.get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting to " + action, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to " + action, e);
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
