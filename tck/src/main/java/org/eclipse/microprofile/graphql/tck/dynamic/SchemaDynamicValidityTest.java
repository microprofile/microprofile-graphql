/*
 * Copyright (c) 2020 Contributors to the Eclipse Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.eclipse.microprofile.graphql.tck.dynamic;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.microprofile.graphql.tck.dynamic.schema.SchemaTestDataProvider;
import org.eclipse.microprofile.graphql.tck.dynamic.schema.TestData;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.RunAsClient;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests that the schema is available at graphql/schema.graphql and that it contains the proper content.
 *
 * @author Phillip Kruger (phillip.kruger@redhat.com)
 */
public class SchemaDynamicValidityTest extends Arquillian {
    private static final Logger LOG = Logger.getLogger(SchemaDynamicValidityTest.class.getName());
    private static final String FILENAME = "schema.graphql";
    private static final String PATH = "graphql/" + FILENAME;

    private String schema;

    @ArquillianResource
    private URI uri;

    @Deployment
    public static Archive<?> getDeployment() throws Exception {
        return DeployableUnit.getDeployment("tck-schematest");
    }

    @RunAsClient
    @Test(priority = 1)
    public void testResponse() throws IOException {
        LOG.log(Level.INFO, "Fetching schema from {0}", uri);
        this.schema = getSchemaContent();
        saveSchemaFile();

        // Check that there is some content
        Assert.assertTrue(schema.length() > 0, "No Content in the GraphQL Schema downloaded from [" + uri + "]");
    }

    @RunAsClient
    @Test(priority = 2, dataProvider = "schemaSnippets", dataProviderClass = SchemaTestDataProvider.class)
    public void testPartsOfSchema(TestData input) {
        Assert.assertNotNull(schema, "No schema avalailable to test against");
        String snippet = schema; // default search against the whole schema
        if (input.getSnippetSearchTerm() != null) {
            snippet = getSchemaSnippet(schema, input.getSnippetSearchTerm());
        }

        Assert.assertTrue(matchAtLeastOneOfTheSnippets(input, snippet),
                "[" + input.getHeader() + "] " + input.getErrorMessage());
    }

    private boolean matchAtLeastOneOfTheSnippets(TestData input, String snippet) {
        List<String> containsAnyOfString = input.getContainsAnyOfString();
        for (String contains : containsAnyOfString) {
            if (contains.startsWith("!")) {
                contains = contains.substring(1);
                if (!snippet.contains(contains)) {
                    return true;
                }
            } else {
                if (snippet.contains(contains)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void saveSchemaFile() {
        try {
            Path downloadedSchema = Paths.get("target" + FS + FILENAME);
            Path createFile = Files.createFile(downloadedSchema);
            try (BufferedWriter writer = Files.newBufferedWriter(createFile, Charset.forName("UTF-8"))) {
                writer.write(this.schema);
            }
            LOG.log(Level.INFO, "Schema written to {0}", createFile.toUri());
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Could not save schema file to target" + FS + FILENAME + " - {0}", ex.getMessage());
        }
    }

    private String getSchemaContent() throws MalformedURLException, ProtocolException, IOException {
        URL url = new URL(this.uri + PATH);
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) url.openConnection();

            connection.setRequestMethod("GET");
            connection.setRequestProperty("Content-Type", "text/plain");

            // Check the response code
            int responseCode = connection.getResponseCode();
            Assert.assertEquals(responseCode, 200, "While testing [" + url.toString() + "]");

            return getContent(connection);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private String getSchemaSnippet(String schema, String section) {
        int index = schema.indexOf(section);
        Assert.assertTrue(index > -1, "Cannot find " + section + " in schema");
        char[] schemaChars = schema.toCharArray();
        int closePos = schema.indexOf("{", index) + 1;
        int counter = 1;
        while (counter > 0 && closePos < schemaChars.length - 1) {
            char c = schemaChars[++closePos];
            if (c == '{') {
                counter++;
            } else if (c == '}') {
                counter--;
            }
        }

        return schema.substring(index, closePos);
    }

    private String getContent(HttpURLConnection connection) throws IOException {
        try (StringWriter sw = new StringWriter();
                BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                sw.write(inputLine);
                sw.write("\n");
            }
            return sw.toString();
        }
    }

    private static final String FS = System.getProperty("file.separator");
}
