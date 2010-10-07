/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.datastore.redis.core;

import java.util.List;
import java.util.Map;

/**
 * Key value operations with 'friendly' names instead of using command names for methods.
 * Additional helper methods for working with keys and values
 * 
 * @author Mark Pollack
 *
 */
public interface KeyValueOperations {

	// Set and Set with expiry operations
	
	void set(String key, String value);
	
	void set(String key, String value, long expiryInMillis);
	
	void setAsBytes(String key, byte[] value);
	
	void setAsBytes(String key, byte[] value, long expiryInMillis);
	
	void convertAndSet(String key, Object value);
	
	void convertAndSet(String key, Object value, long expiryInMillis);

	// Get operations
		
	String get(String key);
	
	byte[] getAsBytes(String key);
	
	<T> T getAndConvert(String key, Class<T> requiredType);
	
	// Get and Set operations
	
	String getAndSet(String key, String value);
	
	byte[] getAndSetBytes(String key, byte[] value);
	
	<T> T getAndSetObject(String key, T value, Class<T> requiredType);
	
	// Multi-get operations
	
	List<String> getValues(List<String> keys);
	
	<T> List<T> getAndConvertValues(List<String> keys, Class<T> requiredType); 

	
	// Set if non-existent operations
	
	void setIfKeyNonExistent(String key, String value);
	
	void setIfKeyNonExistent(String key, byte[] value);
	
	void convertAndSetIfKeyNonExistent(String key, Object value);
	
	// Multiple key-value set
	
	void setMultiple(Map<String, String> keysAndValues);
	
	void setMultipleAsBytes(Map<String, byte[]> keysAndValues);
	
	<T> void convertAndSetMultiple(Map<String, T> keysAndValues);
	
	// Multiple key-value set if non-existent
	
	void setMultipleIfKeysNonExistent(Map<String, String> keysAndValues);
	
	void setMultipleAsBytesIfKeysNonExistent(Map<String, byte[]> keysAndValues);
	
	<T> void convertAndSetMultipleIfKeysNonExistent(Map<String, T> keysAndValues);
			

	
	// Append
	
	int append(String key, String value);
	
	
	
	// Increment
	
	int increment(String key);
	
	int incrementBy(String key, int value);
	
	// Decrement
	
	int decrement(String key);
	
	int decrementBy(String key, int value);
	
	
	// Substring
	
	String getSubString(String key, int fromIndex, int toIndex);
	
	boolean containsKey(String key);
	
	boolean deleteKeys(String... keys);
	
	
	
	
	
	
	
}
