/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.common.base;

/**
 * A convenience typedef that also ties into google's {@code Supplier}.
 *
 * @param <T> The supplied type.
 *
 * @author John Sirois
 */
public interface Supplier<T>
    extends ExceptionalSupplier<T, RuntimeException>, com.google.common.base.Supplier<T> {

  @Override
  T get();
}
