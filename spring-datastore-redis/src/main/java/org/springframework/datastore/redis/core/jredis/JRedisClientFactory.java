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
package org.springframework.datastore.redis.core.jredis;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.jredis.ClientRuntimeException;
import org.jredis.connector.ConnectionSpec;
import org.jredis.ri.alphazero.JRedisClient;
import org.jredis.ri.alphazero.connection.DefaultConnectionSpec;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.datastore.redis.core.AbstractRedisClientFactory;
import org.springframework.datastore.redis.core.RedisClient;
import org.springframework.datastore.redis.support.RedisPersistenceExceptionTranslator;

public class JRedisClientFactory extends AbstractRedisClientFactory {

	public static final String DEFAULT_CHARSET = "UTF-8";

	private volatile String defaultCharset = DEFAULT_CHARSET;


	private RedisPersistenceExceptionTranslator exceptionTranslator;

	private ConnectionSpec connectionSpec;

	public JRedisClientFactory() {
		setHostName(getDefaultHostName());
		exceptionTranslator = new JRedisPersistenceExceptionTranslator();
	}

	public JRedisClientFactory(ConnectionSpec connectionSpec) {
		this.connectionSpec = connectionSpec;
	}

	@Override
	public RedisClient doGetClient() {
		JRedisClient jredis;
		if (connectionSpec == null) {
		
			connectionSpec = DefaultConnectionSpec.newSpec();

			InetAddress address;
			try {
				address = InetAddress.getByName(getHostName());
			} catch (UnknownHostException e) {
				throw new ClientRuntimeException("unknown host: "
						+ getHostName(), e);
			}
			connectionSpec.setAddress(address);

			if (getPort() != 0) {
				connectionSpec.setPort(getPort());
			}
			
			if (getPassword() != null) {
				connectionSpec.setCredentials(stringToByte(getPassword()));
			}			
		}
		
		jredis = new JRedisClient(connectionSpec);		
		return new JRedisSpringClient(jredis, getExceptionTranslator());
	}
	
	protected byte[] stringToByte(String string) throws InvalidDataAccessApiUsageException {
		try {
			return string.getBytes(this.defaultCharset);
		} catch (UnsupportedEncodingException e) {
			throw new InvalidDataAccessApiUsageException(defaultCharset
					+ " encoding not supported.", e);
		}
	}
	
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

	@Override
	public RedisPersistenceExceptionTranslator getExceptionTranslator() {
		return exceptionTranslator;
	}

	public void setExceptionTranslator(
			RedisPersistenceExceptionTranslator exceptionTranslator) {
		this.exceptionTranslator = exceptionTranslator;
	}

}
