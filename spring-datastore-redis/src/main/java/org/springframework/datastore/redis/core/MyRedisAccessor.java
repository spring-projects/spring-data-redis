/*
 * Copyright 2006-2009 the original author or authors.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.datastore.redis.core.connection.RedisConnectionFactory;
import org.springframework.util.Assert;

/**
 * @author Costin Leau
 */
public class MyRedisAccessor implements InitializingBean {

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private RedisConnectionFactory connectionFactory;

	public void afterPropertiesSet() {
		Assert.notNull(getConnectionFactory(), "RedisConnectionFactory is required");
	}

	/**
	 * Returns the connectionFactory.
	 *
	 * @return Returns the connectionFactory
	 */
	public RedisConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	/**
	 * Sets the connection factory.
	 * 
	 * @param connectionFactory The connectionFactory to set.
	 */
	public void setConnectionFactory(RedisConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	public RuntimeException tryToConvertRedisAccessException(Exception ex) {
		throw new UnsupportedOperationException("wire this into dialects/XXXClient utils");
	}
}