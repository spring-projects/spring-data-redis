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
package org.springframework.datastore.redis.util;

import org.springframework.datastore.redis.connection.RedisCommands;


/**
 * Basic interface for Redis-based collections.
 * 
 * @author Costin Leau
 */
public interface RedisStore {

	/**
	 * Returns the key used by the backing Redis store for this collection.
	 *  
	 * @return Redis key
	 */
	String getKey();

	/**
	 * Returns the underlying Redis commands used by the backing implementation.
	 * 
	 * @return commands
	 */
	RedisCommands getCommands();
}
