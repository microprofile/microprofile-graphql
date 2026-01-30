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
package org.eclipse.microprofile.graphql.tck.apps.superhero.api;

import java.io.IOException;

/**
 * Extending a IOException to test transitive blacklist
 *
 * @author Phillip Kruger (phillip.kruger@redhat.com)
 */
public class CsvIOException extends IOException {

    public CsvIOException() {
    }

    public CsvIOException(String string) {
        super(string);
    }

    public CsvIOException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    public CsvIOException(Throwable thrwbl) {
        super(thrwbl);
    }

}
