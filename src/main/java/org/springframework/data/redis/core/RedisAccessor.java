/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.redis.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Base class for {@link RedisTemplate} defining common properties. Not intended to be used directly.
 *
 * @author Costin Leau
 */
public class RedisAccessor implements InitializingBean {

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private @Nullable RedisConnectionFactory connectionFactory;

	public void afterPropertiesSet() {
		Assert.state(getConnectionFactory() != null, "RedisConnectionFactory is required");
	}

	/**
	 * Returns the connectionFactory.
	 *
	 * @return Returns the connectionFactory. Can be {@literal null}
	 */
	@Nullable
	public RedisConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	/**
	 * Returns the required {@link RedisConnectionFactory} or throws {@link IllegalStateException} if the connection
	 * factory is not set.
	 *
	 * @return the associated {@link RedisConnectionFactory}.
	 * @throws IllegalStateException if the connection factory is not set.
	 * @since 2.0
	 */
	public RedisConnectionFactory getRequiredConnectionFactory() {

		RedisConnectionFactory connectionFactory = getConnectionFactory();

		if (connectionFactory == null) {
			throw new IllegalStateException("RedisConnectionFactory is required");
		}

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
}
