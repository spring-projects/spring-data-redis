/*
 * Copyright 2026-present the original author or authors.
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

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.JacksonRedisJsonSerializer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisJsonSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Helper class that simplifies Redis JSON data access.
 * <p>
 * Keys are serialized with {@link JdkSerializationRedisSerializer} by default, which produces binary (non human-readable)
 * key representations. Configure a key serializer through {@link #setKeySerializer(RedisSerializer)}.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
public class RedisJsonTemplate<K> extends RedisAccessor implements BeanClassLoaderAware {

	private boolean enableTransactionSupport = false;
	private boolean initialized = false;
	private @Nullable ClassLoader classLoader;

	private @Nullable RedisSerializer<K> keySerializer;
	private @Nullable RedisJsonSerializer jsonSerializer;

	private final JsonOperations<K> jsonOperations = new DefaultJsonOperations<>(this);

	@Override
	@SuppressWarnings("unchecked")
	public void afterPropertiesSet() {

		super.afterPropertiesSet();

		if (keySerializer == null) {
			keySerializer = (RedisSerializer<K>) new JdkSerializationRedisSerializer(classLoader != null ? classLoader : this.getClass().getClassLoader());
		}

		if (jsonSerializer == null && ClassUtils.isPresent("tools.jackson.databind.ObjectMapper", classLoader)) {
			jsonSerializer = JacksonRedisJsonSerializer.createDefault();
		}

		initialized = true;
	}

	/**
	 * Returns whether this template participates in ongoing transactions.
	 *
	 * @return {@literal true} if transaction support is enabled; {@literal false} otherwise.
	 */
	public boolean isEnableTransactionSupport() {
		return enableTransactionSupport;
	}

	/**
	 * Sets whether this template participates in ongoing transactions using {@literal MULTI...EXEC|DISCARD} to keep track
	 * of operations.
	 *
	 * @param enableTransactionSupport {@literal true} to participate in ongoing transactions; {@literal false} to not
	 *                                 track transactions.
	 * @since 4.2
	 */
	public void setEnableTransactionSupport(boolean enableTransactionSupport) {
		this.enableTransactionSupport = enableTransactionSupport;
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	/**
	 * Returns the key serializer used by this template.
	 *
	 * @return the key serializer used by this template.
	 */
	public @Nullable RedisSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	/**
	 * Sets the key serializer to be used by this template.
	 *
	 * @param keySerializer the key serializer to be used by this template.
	 */
	public void setKeySerializer(@Nullable RedisSerializer<K> keySerializer) {
		this.keySerializer = keySerializer;
	}

	/**
	 * Returns the JSON serializer used by this template.
	 *
	 * @return the JSON serializer used by this template
	 */
	public @Nullable RedisJsonSerializer getJsonSerializer() {
		return jsonSerializer;
	}

	/**
	 * Sets the JSON serializer to be used by this template.
	 *
	 * @param jsonSerializer the JSON serializer to be used by this template.
	 */
	public void setJsonSerializer(@Nullable RedisJsonSerializer jsonSerializer) {
		this.jsonSerializer = jsonSerializer;
	}

	/**
	 * Executes the given action within a {@link RedisConnection} obtained from the configured
	 * {@link RedisConnectionFactory}, releasing the connection once the action completes.
	 *
	 * @param <T>    return type
	 * @param action callback object that specifies the Redis action; must not be {@literal null}.
	 * @return object returned by the action.
	 * @since 4.2
	 */
	<T extends @Nullable Object> T execute(RedisCallback<T> action) {

		Assert.isTrue(initialized, "template not initialized; call afterPropertiesSet() before using it");
		Assert.notNull(action, "Callback object must not be null");

		RedisConnectionFactory factory = getRequiredConnectionFactory();
		RedisConnection connection = RedisConnectionUtils.getConnection(factory, enableTransactionSupport);

		try {
			return action.doInRedis(connection);
		} finally {
			RedisConnectionUtils.releaseConnection(connection, factory);
		}
	}

	/**
	 * Returns the operations performed on JSON values.
	 *
	 * @return JSON operations
	 */
	public JsonOperations<K> opsForJson() {
		return jsonOperations;
	}

}
