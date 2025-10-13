/*
 * Copyright 2017-2025 the original author or authors.
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
package org.springframework.data.redis.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.data.redis.test.XstreamOxmSerializerSingleton;
import org.springframework.data.redis.test.condition.RedisDetector;
import org.springframework.data.redis.test.extension.RedisCluster;
import org.springframework.data.redis.test.extension.RedisStandalone;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class CacheTestParams {

	private static Collection<RedisConnectionFactory> connectionFactories() {

		RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
		config.setHostName(SettingsUtils.getHost());
		config.setPort(SettingsUtils.getPort());

		List<RedisConnectionFactory> factoryList = new ArrayList<>(3);

		// Jedis Standalone
		JedisConnectionFactory jedisConnectionFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(RedisStandalone.class);
		factoryList.add(jedisConnectionFactory);

		// Lettuce Standalone
		LettuceConnectionFactory lettuceConnectionFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(RedisStandalone.class);
		factoryList.add(lettuceConnectionFactory);

		if (clusterAvailable()) {

			// Jedis Cluster
			JedisConnectionFactory jedisClusterConnectionFactory = JedisConnectionFactoryExtension
					.getConnectionFactory(RedisCluster.class);
			factoryList
					.add(jedisClusterConnectionFactory);

			// Lettuce Cluster
			LettuceConnectionFactory lettuceClusterConnectionFactory = LettuceConnectionFactoryExtension
					.getConnectionFactory(RedisCluster.class);
			factoryList
					.add(lettuceClusterConnectionFactory);
		}

		return factoryList;
	}

	static Collection<Object[]> justConnectionFactories() {
		return connectionFactories().stream().map(factory -> new Object[] { factory }).collect(Collectors.toList());
	}

	static Collection<Object[]> connectionFactoriesAndSerializers() {

		OxmSerializer oxmSerializer = XstreamOxmSerializerSingleton.getInstance();
		GenericJackson2JsonRedisSerializer jackson2Serializer = new GenericJackson2JsonRedisSerializer();
		GenericJacksonJsonRedisSerializer jacksonSerializer = GenericJacksonJsonRedisSerializer
            .create(it -> it.enableSpringCacheNullValueSupport().enableUnsafeDefaultTyping());
		JdkSerializationRedisSerializer jdkSerializer = new JdkSerializationRedisSerializer();

		return connectionFactories().stream().flatMap(factory -> Arrays.asList( //
				new Object[] { factory, new FixDamnedJunitParameterizedNameForRedisSerializer(jdkSerializer) }, //
				new Object[] { factory, new FixDamnedJunitParameterizedNameForRedisSerializer(jacksonSerializer) }, //
				new Object[] { factory, new FixDamnedJunitParameterizedNameForRedisSerializer(jackson2Serializer) }, //
				new Object[] { factory, new FixDamnedJunitParameterizedNameForRedisSerializer(oxmSerializer) }).stream())
				.collect(Collectors.toList());
	}

	static class FixDamnedJunitParameterizedNameForRedisSerializer/* ¯\_(ツ)_/¯ */ implements RedisSerializer {

		final RedisSerializer serializer;

		FixDamnedJunitParameterizedNameForRedisSerializer(RedisSerializer serializer) {
			this.serializer = serializer;
		}

		@Override
		public byte[] serialize(@Nullable Object value) throws SerializationException {
			return serializer.serialize(value);
		}

		@Override
		public @Nullable Object deserialize(byte @Nullable[] bytes) throws SerializationException {
			return serializer.deserialize(bytes);
		}

		public static RedisSerializer<Object> java() {
			return RedisSerializer.java();
		}

		public static RedisSerializer<Object> java(@Nullable ClassLoader classLoader) {
			return RedisSerializer.java(classLoader);
		}

		public static RedisSerializer<Object> json() {
			return RedisSerializer.json();
		}

		public static RedisSerializer<String> string() {
			return RedisSerializer.string();
		}

		public static RedisSerializer<byte[]> byteArray() {
			return RedisSerializer.byteArray();
		}

		@Override
		public boolean canSerialize(Class type) {
			return serializer.canSerialize(type);
		}

		@Override
		public Class<?> getTargetType() {
			return serializer.getTargetType();
		}

		@Override // Why Junit? Why?
		public String toString() {
			return serializer.getClass().getSimpleName();
		}
	}

	private static boolean clusterAvailable() {
		return RedisDetector.isClusterAvailable();
	}
}
