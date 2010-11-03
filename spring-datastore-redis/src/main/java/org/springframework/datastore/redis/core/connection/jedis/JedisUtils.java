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

package org.springframework.datastore.redis.core.connection.jedis;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.datastore.redis.RedisConnectionFailureException;
import org.springframework.datastore.redis.UncategorizedRedisException;

import redis.clients.jedis.JedisException;

/**
 * Helper class featuring methods for Jedis connection handling, providing support for exception translation. 
 * 
 * @author Costin Leau
 */
public abstract class JedisUtils {

	public static final String OK_CODE = "OK";

	public static DataAccessException convertJedisAccessException(JedisException ex) {
		return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
	}

	public static DataAccessException convertJedisAccessException(RuntimeException ex) {
		if (ex instanceof JedisException) {
			return convertJedisAccessException((JedisException) ex);
		}

		return new UncategorizedRedisException("Unknown exception", ex);
	}

	static DataAccessException convertJedisAccessException(IOException ex) {
		if (ex instanceof UnknownHostException) {
			return new RedisConnectionFailureException("Unknown host " + ex.getMessage(), ex);
		}
		return new RedisConnectionFailureException("Could not connect to Redis server", ex);
	}

	static DataAccessException convertJedisAccessException(TimeoutException ex) {
		throw new RedisConnectionFailureException("Jedis pool timed out. Could not get Redis Connection", ex);
	}
}
