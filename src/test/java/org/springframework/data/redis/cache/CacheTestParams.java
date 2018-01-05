/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.redis.cache;

import static org.springframework.data.redis.connection.ClusterTestVariables.*;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.runners.model.Statement;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.test.util.RedisClusterRule;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 */
class CacheTestParams {

	private static Collection<RedisConnectionFactory> connectionFactories() {

		RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
		config.setHostName(SettingsUtils.getHost());
		config.setPort(SettingsUtils.getPort());

		List<RedisConnectionFactory> factoryList = new ArrayList<>(3);

		// Jedis Standalone
		JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory(config);
		jedisConnectionFactory.afterPropertiesSet();
		factoryList.add(new FixDamnedJunitParameterizedNameForConnectionFactory(jedisConnectionFactory, ""));

		// Lettuce Standalone
		LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory(config);
		lettuceConnectionFactory.afterPropertiesSet();
		factoryList.add(new FixDamnedJunitParameterizedNameForConnectionFactory(lettuceConnectionFactory, ""));

		if (clusterAvailable()) {

			RedisClusterConfiguration clusterConfiguration = new RedisClusterConfiguration();
			clusterConfiguration.addClusterNode(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_1_PORT));

			// Jedis Cluster
			JedisConnectionFactory jedisClusterConnectionFactory = new JedisConnectionFactory(clusterConfiguration);
			jedisClusterConnectionFactory.afterPropertiesSet();
			factoryList
					.add(new FixDamnedJunitParameterizedNameForConnectionFactory(jedisClusterConnectionFactory, "cluster"));

			// Lettuce Cluster
			LettuceConnectionFactory lettuceClusterConnectionFactory = new LettuceConnectionFactory(clusterConfiguration);
			lettuceClusterConnectionFactory.afterPropertiesSet();

			factoryList
					.add(new FixDamnedJunitParameterizedNameForConnectionFactory(lettuceClusterConnectionFactory, "cluster"));
		}

		return factoryList;
	}

	static Collection<Object[]> justConnectionFactories() {
		return connectionFactories().stream().map(factory -> new Object[] { factory }).collect(Collectors.toList());
	}

	static Collection<Object[]> connectionFactoriesAndSerializers() {

		// XStream serializer
		XStreamMarshaller xstream = new XStreamMarshaller();
		xstream.afterPropertiesSet();

		OxmSerializer oxmSerializer = new OxmSerializer(xstream, xstream);
		GenericJackson2JsonRedisSerializer jackson2Serializer = new GenericJackson2JsonRedisSerializer();
		JdkSerializationRedisSerializer jdkSerializer = new JdkSerializationRedisSerializer();

		return connectionFactories()
				.stream().flatMap(factory -> Arrays
						.asList( //
								new Object[] { factory, new FixDamnedJunitParameterizedNameForRedisSerializer(jdkSerializer) }, //
								new Object[] { factory, new FixDamnedJunitParameterizedNameForRedisSerializer(jackson2Serializer) }, //
								new Object[] { factory, new FixDamnedJunitParameterizedNameForRedisSerializer(oxmSerializer) })
						.stream())
				.collect(Collectors.toList());
	}

	@RequiredArgsConstructor
	static class FixDamnedJunitParameterizedNameForConnectionFactory/* ¯\_(ツ)_/¯ */ implements RedisConnectionFactory {

		final @Delegate RedisConnectionFactory connectionFactory;
		final String addon;

		@Override // Why Junit? Why?
		public String toString() {
			return connectionFactory.getClass().getSimpleName() + (StringUtils.hasText(addon) ? " - [" + addon + "]" : "");
		}
	}

	@RequiredArgsConstructor
	static class FixDamnedJunitParameterizedNameForRedisSerializer/* ¯\_(ツ)_/¯ */ implements RedisSerializer {

		final @Delegate RedisSerializer serializer;

		@Override // Why Junit? Why?
		public String toString() {
			return serializer.getClass().getSimpleName();
		}
	}

	private static boolean clusterAvailable() {

		try {
			new RedisClusterRule().apply(new Statement() {
				@Override
				public void evaluate() throws Throwable {

				}
			}, null).evaluate();
		} catch (Throwable throwable) {
			return false;
		}
		return true;
	}
}
