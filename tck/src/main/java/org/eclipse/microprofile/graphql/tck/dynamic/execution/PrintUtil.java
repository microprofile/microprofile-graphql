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
package org.eclipse.microprofile.graphql.tck.dynamic.execution;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonWriter;
import jakarta.json.JsonWriterFactory;
import jakarta.json.stream.JsonGenerator;

/**
 * Print the Test data to output
 *
 * @author Phillip Kruger (phillip.kruger@redhat.com)
 */
public class PrintUtil {
    private static final Logger LOG = Logger.getLogger(PrintUtil.class.getName());

    private PrintUtil() {
    }

    public static void toDisk(TestData testData, String output, Throwable throwable) {
        try {
            String log = toString(testData, output, throwable);
            writeTestFile(testData.getName(), log);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Could not save data to target folder - {0}", ex.getMessage());
        }
    }

    private static String toString(TestData testData, String output, Throwable throwable) {
        try (StringWriter sw = new StringWriter()) {
            sw.write("============= " + testData.getName() + " =============");
            sw.write("\n\n");
            if (throwable != null) {
                sw.write("errorMessage = " + throwable.getMessage());
            } else {
                sw.write("errorMessage = ");
            }
            sw.write("\n\n");
            if (testData.getInput().size() == 1) {
                sw.write("given input = " + testData.getInput().iterator().next());
            } else {
                sw.write("given multiple inputs = \n");
                testData.getInput().stream().forEach(i -> sw.write(i + "\n"));
            }

            sw.write("\n\n");
            sw.write("variables input = " + prettyJson(testData.getVariables()));
            sw.write("\n\n");
            sw.write("http headers input = " + testData.getHttpHeaders());
            sw.write("\n\n");

            if (testData.getOutput().size() == 1) {
                sw.write("expected output = " + prettyJson(testData.getOutput().iterator().next()));
            } else {
                sw.write(
                        "expected output was either of the following = " + String.join("\nOR\n", testData.getOutput()));
            }

            sw.write("\n\n");
            sw.write("received output = " + prettyJson(output));
            sw.write("\n\n");
            return sw.toString();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void writeTestFile(String testName, String data) throws IOException {
        if (data != null && !data.isEmpty()) {
            Path file = Paths.get("target" + FS + testName + ".log");
            Path createFile = Files.createFile(file);
            try (BufferedWriter writer = Files.newBufferedWriter(createFile, Charset.forName("UTF-8"))) {
                writer.write(data);
            }
        }
    }

    private static String prettyJson(String json) {
        if (json != null) {
            JsonReader jr = Json.createReader(new StringReader(json));
            JsonObject jobj = jr.readObject();
            return prettyJson(jobj);
        }
        return null;
    }

    private static String prettyJson(JsonObject jsonObject) {
        if (jsonObject != null) {
            StringWriter sw = new StringWriter();
            Map<String, Object> properties = new HashMap<>(1);
            properties.put(JsonGenerator.PRETTY_PRINTING, true);

            JsonWriterFactory writerFactory = Json.createWriterFactory(properties);
            try (JsonWriter jsonWriter = writerFactory.createWriter(sw)) {
                jsonWriter.writeObject(jsonObject);
            }
            return sw.toString();
        }
        return null;
    }

    private static final String FS = System.getProperty("file.separator");
}
