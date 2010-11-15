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
  <K, V> KeyValueStoreOperations set(K key, V value);

  <K> KeyValueStoreOperations setAsBytes(K key, byte[] value);

  // Get operations
  <K, V> V get(K key);

  <K> byte[] getAsBytes(K key);

  <K, T> T getAsType(K key, Class<T> requiredType);

  // Get and Set operations
  <K, V> V getAndSet(K key, V value);

  <K> byte[] getAndSetAsBytes(K key, byte[] value);

  <K, V, T> T getAndSetAsType(K key, V value, Class<T> requiredType);

  // Multi-get operations
  <K, V> List<V> getValues(List<K> keys);

  <K, V> List<V> getValues(K... keys);

  <K, T> List<T> getValuesAsType(List<K> keys, Class<T> requiredType);

  <T, K> List<T> getValuesAsType(Class<T> requiredType, K... keys);

  // Set if non-existent operations
  <K, V> KeyValueStoreOperations setIfKeyNonExistent(K key, V value);

  <K> KeyValueStoreOperations setIfKeyNonExistentAsBytes(K key, byte[] value);

  // Multiple key-value set
  <K, V> KeyValueStoreOperations setMultiple(Map<K, V> keysAndValues);

  <K> KeyValueStoreOperations setMultipleAsBytes(Map<K, byte[]> keysAndValues);

  // Multiple key-value set if non-existent
  <K, V> KeyValueStoreOperations setMultipleIfKeysNonExistent(Map<K, V> keysAndValues);

  <K> KeyValueStoreOperations setMultipleAsBytesIfKeysNonExistent(Map<K, byte[]> keysAndValues);

  <K> boolean containsKey(K key);

  <K> boolean deleteKeys(K... keys);

}
