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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a field as a ID Scalar Type. <br>
 * Note that this annotation may only be placed on <code>String</code> fields/getters/setters/parameters. A deployment
 * error should result if it is placed on a field/getter/setter/parameter of a different type.
 *
 * <br>
 * <br>
 * For example, a user might annotate a method's parameter as such:
 *
 * <pre>
 * public class Person {
 *      {@literal @}Id
 *      private String id;
 *      private String name;
 *      private String surname;
 *
 *      // Getters and Setters
 * }
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER, ElementType.FIELD, ElementType.METHOD})
public @interface Id {

}
