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
package org.eclipse.microprofile.graphql.tck.apps.superhero.model;

import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.NonNull;

@Description("A magic source that provides a super hero with powers")
public class Magic implements PowerSource {

  private boolean canBeLearned = false;

  @NonNull
  @Description("Can this magic be learned, or must it be given or obtained innately")
  public boolean canBeLearned() {
    return canBeLearned;
  }

  public void setCanBeLearned(boolean canBeLearned) {
    this.canBeLearned = canBeLearned;
  }
}
