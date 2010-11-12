/*
 * Copyright 2010 the original author or authors.
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

/**
 * Redis store for a certain key. Useful for creating views into Redis 'collection' types.
 *  
 * @author Costin Leau
 */
public interface KeyBound<K> {

	/**
	 * Returns the key associated with this store.
	 * 
	 * @return
	 */
	K getKey();
}
