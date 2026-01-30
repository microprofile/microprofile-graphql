/*
 * Copyright 2020, 2021 Contributors to the Eclipse Foundation
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
package org.eclipse.microprofile.graphql.tck.dynamic;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.microprofile.graphql.tck.dynamic.execution.GraphQLTestDataProvider;
import org.eclipse.microprofile.graphql.tck.dynamic.execution.PrintUtil;
import org.eclipse.microprofile.graphql.tck.dynamic.execution.TestData;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.RunAsClient;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONAssert;
import org.testng.Assert;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonPatchBuilder;
import jakarta.json.JsonReader;

/**
 * This test runs all test defined in the implementation 'src/test/resources' folder and all test included here in the
 * archive's /tests folder
 *
 * @author Phillip Kruger (phillip.kruger@redhat.com)
 */
public class ExecutionDynamicTest extends Arquillian {
    private static final Logger LOG = Logger.getLogger(ExecutionDynamicTest.class.getName());

    private static final String PATH = "graphql"; // Default. TODO: Test when configured

    private static final String UTF8 = "utf-8";
    private static final String MEDIATYPE_JSON = "application/json";
    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final String HEADER_ACCEPT = "Accept";
    private static final String QUERY = "query";
    private static final String VARIABLES = "variables";
    private static final int CONNECT_TIMEOUT = Integer.getInteger("mp.tck.connect.timeout", 5000);
    private static final int READ_TIMEOUT = Integer.getInteger("mp.tck.read.timeout", 5000);

    private TestData currentTestData = null;
    private String currentOutput = null;

    @ArquillianResource
    private URI uri;

    @Deployment
    public static Archive<?> getDeployment() throws Exception {
        return DeployableUnit.getDeployment("tck-dynamic");
    }

    @RunAsClient
    @Test(dataProvider = "specification", dataProviderClass = GraphQLTestDataProvider.class)
    public void testSpecification(TestData testData) {
        runTest(testData);
    }

    @RunAsClient
    @Test(dataProvider = "implementation", dataProviderClass = GraphQLTestDataProvider.class)
    public void testImplementationSpecific(TestData testData) throws IOException {
        runTest(testData);
    }

    private void runTest(TestData testData) {
        if (testData != null && isValidInput(testData.getInput())) {
            LOG.info("Running test [" + testData.getName() + "]");
            this.currentTestData = testData;
            Map<String, String> httpHeaders = new HashMap<>();
            if (testData.getHttpHeaders() != null && !testData.getHttpHeaders().isEmpty()) {
                for (Map.Entry<String, String> header : httpHeaders.entrySet()) {
                    httpHeaders.put(header.getKey(), header.getValue());
                }
            }

            // loop through each of the input content and pass if at least one passes
            boolean success = false;
            ArrayList<Throwable> listExceptions = new ArrayList<>();
            for (String input : testData.getInput()) {
                try {
                    // Prepare if needed
                    if (isValidInput(testData.getPrepare())) {
                        postHTTPRequest(testData.getPrepare(), testData.getVariables(), httpHeaders);
                    }

                    assertTest(input, testData, httpHeaders);
                    success = true;
                    break;
                } catch (AssertionError ae) {
                    // don't raise assertion failure as this is checked below
                    listExceptions.add(ae);
                }
            }
            if (!success) {
                Assert.fail(getErrorMessages(listExceptions));
            }
        } else {
            clearGlobals();
            LOG.warning("Could not find any tests to run...");
        }
    }

    private void assertTest(String input, TestData testData, Map<String, String> httpHeaders) {
        // Run the actual test and get the response
        HttpResponse httpResponse = postHTTPRequest(input, testData.getVariables(), httpHeaders);
        if (httpResponse.isSuccessful()) {
            this.currentOutput = httpResponse.getContent();

            // Validate the output structure
            validateResponseStructure();

            // Cleanup if needed
            if (isValidInput(testData.getCleanup())) {
                postHTTPRequest(testData.getCleanup(), testData.getVariables(), httpHeaders);
            }

            boolean success = false;
            ArrayList<Throwable> listExceptions = new ArrayList<>();

            // Compare to expected output and pass if at least one of the output files match
            for (String output : testData.getOutput()) {
                try {
                    JSONAssert.assertEquals(testData.getFailMessage(), output, this.currentOutput, testData.beStrict());
                    success = true;
                    break;
                } catch (AssertionError ex) {
                    // don't raise assertion failure as this is checked below
                    listExceptions.add(ex);
                } catch (JSONException je) {
                    // indicates some sort of JSON formatting exception
                    Assert.fail(je.getMessage());
                }
            }
            if (!success) {
                Assert.fail(getErrorMessages(listExceptions));
            }
        } else {
            Assert.assertEquals(httpResponse.status, testData.getExpectedHttpStatusCode(), httpResponse.getContent());
        }
    }

    @AfterMethod
    public void tearDown(ITestResult result) {
        if (result != null && result.getStatus() == ITestResult.FAILURE) {
            PrintUtil.toDisk(this.currentTestData,
                    this.currentOutput,
                    result.getThrowable());
        }
        clearGlobals();
    }

    private void validateResponseStructure() {
        JsonObject received = getJsonObject(new StringReader(this.currentOutput));
        JsonArray errors = received.getJsonArray("errors");
        validatePart(received, "root", "data", "errors");
        if (errors != null) {
            for (JsonObject errorJsonObject : errors.getValuesAs(JsonObject.class)) {
                JsonArray locations = errorJsonObject.getJsonArray("locations");
                validatePart(errorJsonObject, "errors", "message", "locations", "path", "extensions");
                if (locations != null) {
                    for (JsonObject locationJsonObject : locations.getValuesAs(JsonObject.class)) {
                        validatePart(locationJsonObject, "errors/locations", "line", "column");
                    }
                }
            }
        }
    }

    private void validatePart(JsonObject jsonObject, String rootName, String... keys) {
        JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();
        for (String key : keys) {
            jsonPatchBuilder = remove(jsonPatchBuilder, jsonObject, key);
        }

        JsonObject emptyObject = jsonPatchBuilder.build().apply(jsonObject);

        Assert.assertTrue(emptyObject.isEmpty(),
                "Unknown elements in " + rootName + ", only " + Arrays.toString(keys) + " expected");
    }

    private JsonPatchBuilder remove(JsonPatchBuilder emptyJsonErrorPatchBuilder, JsonObject errorJsonObject,
            String key) {
        if (errorJsonObject.containsKey(key)) {
            emptyJsonErrorPatchBuilder = emptyJsonErrorPatchBuilder.remove("/" + key);
        }
        return emptyJsonErrorPatchBuilder;
    }

    private void clearGlobals() {
        this.currentTestData = null;
        this.currentOutput = null;
    }

    private JsonObject getJsonObject(Reader input) {
        JsonReader expectedReader = Json.createReader(input);
        return expectedReader.readObject();
    }

    private boolean isValidInput(String input) {
        return input != null && !input.isEmpty();
    }

    private boolean isValidInput(Set<String> setInput) {
        for (String input : setInput) {
            if (!isValidInput(input)) {
                return false;
            }
        }
        return true;
    }

    private String getErrorMessages(ArrayList<Throwable> listExceptions) {
        StringBuilder sb = new StringBuilder();
        listExceptions.forEach(ex -> sb.append(ex.getMessage()).append('\n'));
        return sb.toString();
    }

    private HttpResponse postHTTPRequest(String graphQL, JsonObject variables, Map<String, String> httpHeaders) {
        try {
            URL url = new URL(this.uri + PATH);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST"); // TODO: Also test with GET and query string ? Are we allowing it ?

            setTimeouts(connection);
            addHeaders(connection, httpHeaders);

            connection.setDoOutput(true);

            JsonObject body = createRequestBody(graphQL, variables);

            postRequest(connection, body);

            int status = connection.getResponseCode();

            if (status == 200) {
                return new HttpResponse(status, getResponse(connection));
            } else {
                return new HttpResponse(status, connection.getResponseMessage());
            }

        } catch (ProtocolException pex) {
            LOG.log(Level.SEVERE, "Caught ProtocolException attempting to post an HTTP request", pex);
            throw new RuntimeException(pex);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Caught IOException attempting to post an HTTP request", ex);
            Assert.fail("Could not open a connection to the test server, is it running ?");
            throw new RuntimeException(ex);
        }
    }

    private void addHeaders(HttpURLConnection connection, Map<String, String> httpHeaders) {
        // Default headers
        connection.setRequestProperty(HEADER_CONTENT_TYPE, MEDIATYPE_JSON); // default header.
        connection.setRequestProperty(HEADER_ACCEPT, MEDIATYPE_JSON);

        // Provided headers
        if (httpHeaders != null) {
            for (Map.Entry<String, String> header : httpHeaders.entrySet()) {
                connection.setRequestProperty(header.getKey(), header.getValue());
            }
        }
    }

    private void setTimeouts(HttpURLConnection connection) {
        // Set timeouts
        connection.setConnectTimeout(CONNECT_TIMEOUT);
        connection.setReadTimeout(READ_TIMEOUT);
    }

    private JsonObject createRequestBody(String graphQL, JsonObject variables) {
        // Create the request
        if (variables == null || variables.isEmpty()) {
            variables = Json.createObjectBuilder().build();
        }
        return Json.createObjectBuilder().add(QUERY, graphQL).add(VARIABLES, variables).build();
    }

    private void postRequest(HttpURLConnection connection, JsonObject body) throws IOException {
        try (OutputStream os = connection.getOutputStream()) {
            byte[] input = body.toString().getBytes(UTF8);
            os.write(input, 0, input.length);
        }
    }

    private String getResponse(HttpURLConnection connection) throws IOException {
        int status = connection.getResponseCode();
        if (status == 200) {
            // Read the response
            try (BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(), UTF8))) {
                StringBuilder response = new StringBuilder();
                String responseLine;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
                String jsonResult = response.toString();
                connection.disconnect();
                return jsonResult;
            }
        } else {
            connection.disconnect();
            throw new RuntimeException("Status " + status + " - " + connection.getResponseMessage());
        }
    }

    private static class HttpResponse {
        private final int status;
        private final String content;

        public HttpResponse(int status, String content) {
            this.status = status;
            this.content = content;
        }

        public int getStatus() {
            return status;
        }

        public String getContent() {
            return content;
        }

        public boolean isSuccessful() {
            return status == 200;
        }
    }
}
