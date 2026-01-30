/*
 * Copyright (c) 2020, 2021 Contributors to the Eclipse Foundation
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
import java.io.StringWriter;
import java.util.Properties;

import org.eclipse.microprofile.graphql.ConfigKey;
import org.eclipse.microprofile.graphql.tck.apps.basic.api.ScalarTestApi;
import org.eclipse.microprofile.graphql.tck.apps.superhero.api.HeroFinder;
import org.eclipse.microprofile.graphql.tck.apps.superhero.db.HeroDatabase;
import org.eclipse.microprofile.graphql.tck.apps.superhero.model.SuperHero;
import org.eclipse.microprofile.graphql.tck.dynamic.execution.GraphQLTestDataProvider;
import org.eclipse.microprofile.graphql.tck.dynamic.schema.SchemaTestDataProvider;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;

/**
 * Helper to create the deployable unit to test against
 *
 * @author Phillip Kruger (phillip.kruger@redhat.com)
 */
public class DeployableUnit {

    private DeployableUnit() {
    }

    public static Archive<?> getDeployment(String unitName) throws Exception {
        return ShrinkWrap.create(WebArchive.class, unitName + ".war")
                .addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml")
                .addAsManifestResource(
                        new StringAsset(getPropertyAsString()),
                        "microprofile-config.properties")
                .addPackage(HeroFinder.class.getPackage())
                .addPackage(HeroDatabase.class.getPackage())
                .addPackage(SuperHero.class.getPackage())
                .addPackage(ScalarTestApi.class.getPackage())
                .addPackage(SchemaTestDataProvider.class.getPackage())
                .addPackage(GraphQLTestDataProvider.class.getPackage());
    }

    private static String getPropertyAsString() throws IOException {
        StringWriter writer = new StringWriter();
        PROPERTIES.store(writer, "TCK Properties");
        return writer.toString();
    }

    private static final Properties PROPERTIES = new Properties();
    static {
        PROPERTIES.put(ConfigKey.DEFAULT_ERROR_MESSAGE,
                "Unexpected failure in the system. Jarvis is working to fix it.");
        PROPERTIES.put(ConfigKey.EXCEPTION_BLACK_LIST, "java.io.IOException,java.util.concurrent.TimeoutException");
        PROPERTIES.put(ConfigKey.EXCEPTION_WHITE_LIST,
                "org.eclipse.microprofile.graphql.tck.apps.superhero.api.WeaknessNotFoundException");
    }

}
