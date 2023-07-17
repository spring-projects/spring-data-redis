/*
 * Copyright 2011-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
import org.springframework.data.redis.util.RedisAssertions;
import org.springframework.lang.Nullable;

/**
 * Base class for {@link RedisTemplate} defining common properties. Not intended to be used directly.
 *
 * @author Costin Leau
 * @author John Blum
 * @see org.springframework.beans.factory.InitializingBean
 */
public abstract class RedisAccessor implements InitializingBean {

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private @Nullable RedisConnectionFactory connectionFactory;

	@Override
	public void afterPropertiesSet() {
		getRequiredConnectionFactory();
	}

	/**
	 * Returns the factory configured to acquire connections and perform operations on the connected Redis instance.
	 *
	 * @return the configured {@link RedisConnectionFactory}. Can be {@literal null}.
	 * @see RedisConnectionFactory
	 */
	@Nullable
	public RedisConnectionFactory getConnectionFactory() {
		return this.connectionFactory;
	}

	/**
	 * Returns the required {@link RedisConnectionFactory}, throwing an {@link IllegalStateException}
	 * if the {@link RedisConnectionFactory} is not set.
	 *
	 * @return the configured {@link RedisConnectionFactory}.
	 * @throws IllegalStateException if the {@link RedisConnectionFactory} is not set.
	 * @see #getConnectionFactory()
	 * @since 2.0
	 */
	public RedisConnectionFactory getRequiredConnectionFactory() {
		return RedisAssertions.requireState(getConnectionFactory(), "RedisConnectionFactory is required");
	}

	/**
	 * Sets the factory used to acquire connections and perform operations on the connected Redis instance.
	 *
	 * @param connectionFactory {@link RedisConnectionFactory} used to acquire connections.
	 * @see RedisConnectionFactory
	 */
	public void setConnectionFactory(@Nullable RedisConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}
}
