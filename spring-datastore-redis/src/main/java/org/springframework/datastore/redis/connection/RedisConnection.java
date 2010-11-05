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

package org.springframework.datastore.redis.connection;

import org.springframework.datastore.redis.UncategorizedRedisException;

/**
 * A connection (session) to a Redis server.
 * The methods namings follows as much as possible the Redis conventions.  
 *  
 * @author Costin Leau
 */
public interface RedisConnection extends RedisCommands, RedisHashCommands, RedisListCommands, RedisSetCommands,
		RedisStringCommands, RedisZSetCommands {

	/**
	 * Close (or quit) the connection.
	 * 
	 * @throws UncategorizedRedisException
	 */
	void close() throws UncategorizedRedisException;

	boolean isClosed();

	Object getNativeConnection();

	String getEncoding();

	/**
	 * Indicates whether the connection is in "queue"(or "MULTI") mode or not.
	 * When queueing, all commands are postponed until EXEC or DISCARD commands
	 * are issued.
	 * Since in queueing, no results are returned, the connection will return NULL
	 * on all operations that interact with the data. 
	 * 
	 * @return true if the connection is in queue/MULTI mode, false otherwise
	 */
	boolean isQueueing();
}
