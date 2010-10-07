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

import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.InvalidDataAccessApiUsageException;

/**
 * Common base class for RedisClient implementations
 * @author Mark Pollack
 *
 */
public abstract class AbstractRedisClient implements RedisClient {

	protected final Log logger = LogFactory.getLog(this.getClass());
	
	public static final String DEFAULT_CHARSET = "UTF-8";

	private volatile String defaultCharset = DEFAULT_CHARSET;

	/**
	 * Specify the default charset to use when converting to or from text-based
	 * Message body content. If not specified, the charset will be "UTF-8".
	 */
	public void setDefaultCharset(String defaultCharset) {
		this.defaultCharset = (defaultCharset != null) ? defaultCharset : DEFAULT_CHARSET;
	}
	
	public String getDefaultCharset() {
		return defaultCharset;
	}
	
	protected byte[] stringToByte(String string) throws InvalidDataAccessApiUsageException {
		try {
			return string.getBytes(this.defaultCharset);
		} catch (UnsupportedEncodingException e) {
			throw new InvalidDataAccessApiUsageException(defaultCharset
					+ " encoding not supported.", e);
		}
	}

	protected String byteToString(byte[] value) throws InvalidDataAccessApiUsageException {
		try {
			return new String(value, defaultCharset);
		} catch (UnsupportedEncodingException e) {
			throw new InvalidDataAccessApiUsageException(defaultCharset
					+ " encoding not supported.", e);
		}
	}
}
