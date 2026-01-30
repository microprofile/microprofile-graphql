/*
 * Copyright 2020 Contributors to the Eclipse Foundation
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
package org.eclipse.microprofile.graphql.tck.dynamic.schema;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.microprofile.graphql.tck.dynamic.DynamicPaths;
import org.testng.annotations.DataProvider;

/**
 * Provide test data for GraphQL Schema. This will find and load all csv files in the test folder and use that as test
 * data against the schema
 *
 * @author Phillip Kruger (phillip.kruger@redhat.com)
 */
public class SchemaTestDataProvider {
    private static final Logger LOG = Logger.getLogger(SchemaTestDataProvider.class.getName());

    private SchemaTestDataProvider() {
    }

    @DataProvider(name = "schemaSnippets")
    public static Object[][] getSnippetTestData() {
        List<Path> dataFiles = getDataFiles();

        List<TestData> testDataList = toListOfTestData(dataFiles);

        Object[][] testParameters = new Object[testDataList.size()][1];

        for (int row = 0; row < testDataList.size(); row++) {
            TestData testData = testDataList.get(row);
            testParameters[row][0] = testData;
        }

        return testParameters;
    }

    private static List<Path> getDataFiles() {
        List<Path> f = new ArrayList<>();

        // Implementation specific tests
        try {
            f.addAll(toListOfPaths(DynamicPaths.getDataForImplementation()));
        } catch (IOException ex) {
            LOG.log(Level.INFO, "No implementation specific tests found [{0}]", ex.getMessage());
        }

        // Specification test
        if (!disableSpecificationTests()) {
            try {
                f.addAll(toListOfPaths(DynamicPaths.getDataForSpecification()));
            } catch (Exception ex) {
                LOG.log(Level.WARNING, "No specification tests found [{0}]", ex.getMessage());
            }
        }

        return f;
    }

    private static List<Path> toListOfPaths(DirectoryStream<Path> directoryStream) {
        List<Path> files = new ArrayList<>();
        for (Path p : directoryStream) {
            if (!Files.isDirectory(p) && p.getFileName().toString().endsWith(FILE_TYPE)) {
                files.add(p);
            }
        }
        return files;
    }

    private static List<TestData> toListOfTestData(List<Path> testFolders) {
        List<TestData> testDataList = new LinkedList<>();
        for (Path testFile : testFolders) {
            try {
                testDataList.addAll(toTestData(testFile));
            } catch (IOException ioe) {
                LOG.log(Level.SEVERE, "Could not add test case {0} - {1}",
                        new Object[]{testFile.getFileName().toString(), ioe.getMessage()});
            }

        }
        return testDataList;
    }

    private static List<TestData> toTestData(Path testFile) throws IOException {
        List<TestData> testDataList = new LinkedList<>();
        List<String> content = Files.readAllLines(testFile);
        String currentHeader = "";
        for (String line : content) {
            if (validLine(line)) {
                String[] parts = line.split(DELIMITER);
                if (parts.length == 4) {
                    TestData testData = createTestData(currentHeader, testFile.getFileName().toString(), parts);
                    testDataList.add(testData);
                } else {
                    LOG.log(Level.SEVERE, "Could not add test case {0} - {1}",
                            new Object[]{testFile.getFileName().toString(),
                                    "Does not contain 3 parts [" + parts.length + "]"});
                }
            } else if (isHeader(line)) {
                currentHeader = line.substring(line.indexOf(COMMENT) + 1).trim();
            }
        }
        return testDataList;
    }

    private static TestData createTestData(String header, String filename, String[] parts) {
        TestData testData = new TestData();
        testData.setCount(Integer.valueOf(parts[0]));
        testData.setHeader(header);
        testData.setName(filename);
        String count = parts[0].trim();
        String snippet = parts[1].trim();
        if (snippet == null || snippet.isEmpty()) {
            snippet = null;
        }
        testData.setSnippetSearchTerm(snippet);

        String containsString = parts[2].trim();
        if (containsString.contains(OR)) {
            String[] containsStrings = containsString.split(OR);
            for (String oneOf : containsStrings) {
                testData.addContainsString(oneOf.trim());
            }
        } else {
            testData.addContainsString(containsString);
        }
        testData.setErrorMessage("(" + count + ") - " + parts[3].trim());

        return testData;
    }

    private static boolean validLine(String line) {
        return !line.isEmpty() && line.trim().contains(PIPE) && !isHeader(line);
    }

    private static boolean isHeader(String line) {
        return line.trim().startsWith(COMMENT);
    }

    private static boolean disableSpecificationTests() {
        return Boolean.valueOf(System.getProperty("disableSpecificationTests", "false"));
    }

    private static final String PIPE = "|";
    private static final String DELIMITER = "\\" + PIPE;
    private static final String COMMENT = "#";
    private static final String FILE_TYPE = ".csv";
    private static final String OR = "'OR'";
}
