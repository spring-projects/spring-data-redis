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

import java.util.Map;
import java.util.Properties;

import org.jredis.ClientRuntimeException;
import org.jredis.RedisException;
import org.jredis.RedisType;
import org.jredis.Sort;
import org.jredis.connector.NotConnectedException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.SortParameters.Order;
import org.springframework.data.redis.connection.SortParameters.Range;

/**
 * Helper class featuring methods for JRedis connection handling, providing support for exception translation.
 * 
 * @author Costin Leau
 * @author Jennifer Hickey
 */
public abstract class JredisUtils {

	/**
	 * Converts the given, native JRedis exception to Spring's DAO hierarchy.
	 * 
	 * @param ex JRedis exception
	 * @return converted exception
	 */
	public static DataAccessException convertJredisAccessException(RedisException ex) {
		return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
	}

	/**
	 * Converts the given, native JRedis exception to Spring's DAO hierarchy.
	 * 
	 * @param ex JRedis exception
	 * @return converted exception
	 */
	public static DataAccessException convertJredisAccessException(ClientRuntimeException ex) {
		if (ex instanceof NotConnectedException) {
			return new RedisConnectionFailureException(ex.getMessage(), ex);
		}
		return new InvalidDataAccessResourceUsageException(ex.getMessage(), ex);
	}

	static DataType convertDataType(RedisType type) {
		switch (type) {
			case NONE:
				return DataType.NONE;
			case string:
				return DataType.STRING;
			case list:
				return DataType.LIST;
			case set:
				return DataType.SET;
				// case zset:
				// return DataType.ZSET;
			case hash:
				return DataType.HASH;
		}

		return null;
	}

	static Sort applySortingParams(Sort jredisSort, SortParameters params, byte[] storeKey) {
		if (params != null) {
			byte[] byPattern = params.getByPattern();
			if (byPattern != null) {
				jredisSort.BY(byPattern);
			}
			byte[][] getPattern = params.getGetPattern();

			if (getPattern != null && getPattern.length > 0) {
				for (byte[] bs : getPattern) {
					jredisSort.GET(bs);
				}
			}
			Range limit = params.getLimit();
			if (limit != null) {
				jredisSort.LIMIT(limit.getStart(), limit.getCount());
			}
			Order order = params.getOrder();
			if (order != null && order.equals(Order.DESC)) {
				jredisSort.DESC();
			}
			Boolean isAlpha = params.isAlphabetic();
			if (isAlpha != null && isAlpha) {
				jredisSort.ALPHA();
			}
		}

		if (storeKey != null) {
			jredisSort.STORE(storeKey);
		}

		return jredisSort;
	}

	static Properties info(Map<String, String> map) {
		Properties info = new Properties();
		info.putAll(map);
		return info;
	}

	static Long toLong(Boolean source) {
		return source ? 1l : 0l;
	}
}
