/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.redis.connection.jredis;

import org.jredis.ClientRuntimeException;
import org.jredis.JRedis;

/**
 * Pool of {@link JRedis} connection objects.
 * 
 * @author Jennifer Hickey
 * 
 */
public interface JredisPool {

	/**
	 * 
	 * @return A {@link JRedis} connection, if available. Throws a
	 *         {@link ClientRuntimeException} if a connection cannot be borrowed
	 *         from the pool
	 */
	JRedis getResource();

	/**
	 * 
	 * @param resource
	 *            A broken {@link JRedis} connection that should be invalidated
	 */
	void returnBrokenResource(final JRedis resource);

	/**
	 * 
	 * @param resource
	 *            A {@link JRedis} connection to return to the pool
	 */
	void returnResource(final JRedis resource);

	/**
	 * Destroys the connection pool
	 */
	void destroy();

}
