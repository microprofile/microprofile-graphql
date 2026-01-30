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
package org.eclipse.microprofile.graphql.tck.dynamic.schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple Holder for Test Data sets
 *
 * @author Phillip Kruger (phillip.kruger@redhat.com)
 */
public class TestData {
    private int count;
    private String header;
    private String name;
    private String snippetSearchTerm;
    private List<String> containsAnyOfString = new ArrayList<>();
    private String errorMessage;

    public TestData() {
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSnippetSearchTerm() {
        return snippetSearchTerm;
    }

    public void setSnippetSearchTerm(String snippetSearchTerm) {
        this.snippetSearchTerm = snippetSearchTerm;
    }

    public List<String> getContainsAnyOfString() {
        return containsAnyOfString;
    }

    public void setContainsAnyOfString(List<String> containsAnyOfString) {
        this.containsAnyOfString = containsAnyOfString;
    }

    public void addContainsString(String containsString) {
        this.containsAnyOfString.add(containsString);
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

}
