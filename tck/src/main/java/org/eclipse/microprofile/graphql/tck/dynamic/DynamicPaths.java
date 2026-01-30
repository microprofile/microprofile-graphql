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

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Where we look for tests in both the Spec level and the implementation level
 *
 * @author Phillip Kruger (phillip.kruger@redhat.com)
 */
public class DynamicPaths {
    private static final String SPEC = "/tests/";
    private static final String IMPL = "src/test/resources/tests";

    private DynamicPaths() {
    }

    public static DirectoryStream<Path> getDataForImplementation() throws IOException {
        Path folderPath = Paths.get(IMPL);
        return Files.newDirectoryStream(folderPath);
    }

    public static DirectoryStream<Path> getDataForSpecification() throws IOException, URISyntaxException {
        URL jar = DynamicPaths.class.getProtectionDomain().getCodeSource().getLocation();
        Path jarFile = Paths.get(jar.toURI());
        FileSystem fs = FileSystems.newFileSystem(jarFile, (ClassLoader) null);
        return Files.newDirectoryStream(fs.getPath(SPEC));
    }
}
