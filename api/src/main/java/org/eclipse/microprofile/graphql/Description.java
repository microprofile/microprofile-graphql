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

package org.eclipse.microprofile.graphql;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Sets the description in the GraphQL schema for the target field, type, parameter, etc. <br>
 * <br>
 * For example, a user might annotate a type and field as such:
 *
 * <pre>
 * {@literal @}Description("Vehicle for traveling between star systems")
 * public class Starship {
 *     private String id;
 *     private float length;
 *     {@literal @}Description("Name of a particular starship, not it's class - i.e. 'Millenium Falcon'")
 *     private String name;
 *
 *     // getters/setters...
 * }
 * </pre>
 *
 * Schema generation of this would result in a stanza such as:
 *
 * <pre>
 * "Vehicle for traveling between star systems"
 * type Starship {
 *   id: String
 *   length: Float
 *   "Name of a particular starship, not it's class - i.e. 'Millenium Falcon'"
 *   name: String
 * }
 * </pre>
 */
@Target({ElementType.PARAMETER, ElementType.METHOD, ElementType.FIELD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Description {

    /**
     * @return the description text.
     */
    String value() default "";
}