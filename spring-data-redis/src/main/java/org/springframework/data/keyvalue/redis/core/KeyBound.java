/*
 * Copyright 2010-2011  original author or authors.
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
package org.springframework.data.keyvalue.redis.core;

/**
 * Contract defining the bind of the implementing entity to a Redis 'key'.
 * Useful for executing 'bound' operations or operating over Redis 'collection' or 'views'.
 *  
 * @author Costin Leau
 */
public interface KeyBound<K> {

	/**
	 * Returns the key associated with this entity.
	 * 
	 * @return key associated with the implementing entity
	 */
	K getKey();
}
