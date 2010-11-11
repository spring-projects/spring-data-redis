/*
 * Copyright (c) 2010 by J. Brisbin <jon@jbrisbin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WIVHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.datastore.riak.core;

import java.util.List;
import java.util.Map;

public interface KeyValueStoreOperations {

  // Set and Set with expiry operations
  <V> KeyValueStoreOperations set(Object key, V value);

  KeyValueStoreOperations setAsBytes(Object key, byte[] value);

  // Get operations
  <V> V get(Object key);

  byte[] getAsBytes(Object key);

  <T> T getAsType(Object key, Class<T> requiredType);

  // Get and Set operations
  <V> V getAndSet(Object key, V value);

  byte[] getAndSetBytes(Object key, byte[] value);

  <T> T getAndSetAsType(Object key, Object value, Class<T> requiredType);

  // Multi-get operations
  List<?> getValues(List<Object> keys);

  List<?> getValues(Object... keys);

  <T> List<T> getValuesAsType(List<Object> keys, Class<T> requiredType);

  <T> List<T> getValuesAsType(Class<T> requiredType, Object... keys);

  // Set if non-existent operations
  <V> KeyValueStoreOperations setIfKeyNonExistent(Object key, V value);

  <V> KeyValueStoreOperations setIfKeyNonExistentAsBytes(Object key, byte[] value);

  // Multiple key-value set
  KeyValueStoreOperations setMultiple(Map<Object, Object> keysAndValues);

  KeyValueStoreOperations setMultipleAsBytes(Map<Object, byte[]> keysAndValues);

  // Multiple key-value set if non-existent
  KeyValueStoreOperations setMultipleIfKeysNonExistent(Map<Object, Object> keysAndValues);

  KeyValueStoreOperations setMultipleAsBytesIfKeysNonExistent(Map<Object, byte[]> keysAndValues);

  boolean containsKey(Object keys);

  boolean deleteKeys(Object... keys);

}
