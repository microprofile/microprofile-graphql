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
 * Specifies that the GraphQL type and/or input type represented by the Java field this annotation is applied to must be
 * marked as non-null in the schema. <br>
 * <br>
 * For example, a user might annotate a class' property as such:
 *
 * <pre>
 * {@literal @}Type("Starship")
 * {@literal @}Input("StarshipInput")
 * public class Starship {
 *     private String id;
 *     {@literal @}NonNull
 *     private String name;
 *     private float length;
 *
 *     // getters/setters...
 * }
 * </pre>
 *
 * Schema generation of this would result in a stanza such as:
 *
 * <pre>
 * type Starship {
 *   id: String
 *   name: String!
 *   length: Float!
 * }
 *
 * input StarshipInput {
 *   id: String
 *   name: String!
 *   length: Float!
 * }
 * </pre>
 *
 * <br>
 * <br>
 * Note that all primitive fields/properties are automatically considered non-null unless they are also annotated with a
 * <code>DefaultValue</code> annotation.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE_USE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
@Documented
public @interface NonNull {
}