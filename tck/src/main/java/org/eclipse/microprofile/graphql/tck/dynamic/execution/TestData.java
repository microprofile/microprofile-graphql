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

import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import jakarta.json.JsonObject;

/**
 * Simple Holder for Test Data sets
 *
 * @author Phillip Kruger (phillip.kruger@redhat.com)
 */
public class TestData {
    private String name;
    private Set<String> input;
    private Properties httpHeaders;
    private Set<String> output;
    private JsonObject variables;
    private String prepare;
    private String cleanup;
    private Properties properties;

    public TestData() {

    }

    public TestData(String name) {
        this.name = name;
        this.output = new HashSet<>();
        this.input = new HashSet<>();
    }

    public TestData(String name,
            Set<String> input,
            Properties httpHeaders,
            Set<String> output,
            JsonObject variables,
            String prepare,
            String cleanup,
            Properties properties) {

        this.name = name;
        this.input = input;
        this.httpHeaders = httpHeaders;
        this.output = output;
        this.variables = variables;
        this.prepare = prepare;
        this.cleanup = cleanup;
        this.properties = properties;
    }

    public String getName() {
        return name;
    }

    public Set<String> getInput() {
        return input;
    }

    public Properties getHttpHeaders() {
        return httpHeaders;
    }

    public Set<String> getOutput() {
        return output;
    }

    public JsonObject getVariables() {
        return variables;
    }

    public String getPrepare() {
        return prepare;
    }

    public String getCleanup() {
        return cleanup;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setInput(Set<String> input) {
        this.input = input;
    }

    public void addInput(String input) {
        this.input.add(input);
    }

    public void setHttpHeaders(Properties httpHeaders) {
        this.httpHeaders = httpHeaders;
    }

    public void setOutput(Set<String> output) {
        this.output = output;
    }

    public void addOutput(String output) {
        this.output.add(output);
    }

    public void setVariables(JsonObject variables) {
        this.variables = variables;
    }

    public void setPrepare(String prepare) {
        this.prepare = prepare;
    }

    public void setCleanup(String cleanup) {
        this.cleanup = cleanup;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public boolean shouldIgnore() {
        return getBooleanProperty("ignore");
    }

    public Integer getPriority() {
        if (properties != null && !properties.isEmpty() && properties.containsKey("priority")) {
            return Integer.valueOf(properties.getProperty("priority"));
        }
        return 999; // default
    }

    public int getExpectedHttpStatusCode() {
        if (properties != null && !properties.isEmpty() && properties.containsKey("expectedHttpStatusCode")) {
            return Integer.valueOf(properties.getProperty("expectedHttpStatusCode"));
        }
        return 200; // default
    }

    public boolean beStrict() {
        return getBooleanProperty("strict");
    }

    public String getFailMessage() {
        if (properties != null && !properties.isEmpty() && properties.containsKey("failMessage")) {
            return properties.getProperty("failMessage");
        }
        return getName() + " failed"; // default
    }

    private boolean getBooleanProperty(String key) {

        if (properties != null && !properties.isEmpty() && properties.containsKey(key)) {
            String ignore = properties.getProperty(key);
            if (ignore != null && !ignore.isEmpty() && ignore.equalsIgnoreCase("true")) {
                return true;
            }
        }
        return false; // default

    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 17 * hash + Objects.hashCode(this.name);
        hash = 17 * hash + Objects.hashCode(this.input);
        hash = 17 * hash + Objects.hashCode(this.httpHeaders);
        hash = 17 * hash + Objects.hashCode(this.output);
        hash = 17 * hash + Objects.hashCode(this.variables);
        hash = 17 * hash + Objects.hashCode(this.prepare);
        hash = 17 * hash + Objects.hashCode(this.cleanup);
        hash = 17 * hash + Objects.hashCode(this.properties);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TestData other = (TestData) obj;
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (!Objects.equals(this.input, other.input)) {
            return false;
        }
        if (!Objects.equals(this.output, other.output)) {
            return false;
        }
        if (!Objects.equals(this.prepare, other.prepare)) {
            return false;
        }
        if (!Objects.equals(this.cleanup, other.cleanup)) {
            return false;
        }
        if (!Objects.equals(this.httpHeaders, other.httpHeaders)) {
            return false;
        }
        if (!Objects.equals(this.variables, other.variables)) {
            return false;
        }
        if (!Objects.equals(this.properties, other.properties)) {
            return false;
        }
        return true;
    }

}
