/*
 * Copyright 2017-2019 the original author or authors.
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

import static org.springframework.data.redis.connection.ClusterTestVariables.*;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.runners.model.Statement;
import org.springframework.data.redis.ByteBufferObjectFactory;
import org.springframework.data.redis.DoubleObjectFactory;
import org.springframework.data.redis.LongObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;
import org.springframework.data.redis.PrefixStringObjectFactory;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientResources;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.test.util.RedisClusterRule;
import org.springframework.oxm.xstream.XStreamMarshaller;

/**
 * Parameters for testing implementations of {@link ReactiveRedisTemplate}
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
abstract public class ReactiveOperationsTestParams {

	public static Collection<Object[]> testParams() {

		LettuceClientConfiguration clientConfiguration = LettuceClientConfiguration.builder() //
				.shutdownTimeout(Duration.ZERO) //
				.clientResources(LettuceTestClientResources.getSharedClientResources()) //
				.build();

		LettucePoolingClientConfiguration poolingConfiguration = LettucePoolingClientConfiguration.builder() //
				.shutdownTimeout(Duration.ZERO) //
				.clientResources(LettuceTestClientResources.getSharedClientResources()) //
				.build();

		RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration(SettingsUtils.getHost(),
				SettingsUtils.getPort());

		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<String> clusterKeyStringFactory = new PrefixStringObjectFactory("{u1}.", stringFactory);
		ObjectFactory<Long> longFactory = new LongObjectFactory();
		ObjectFactory<Double> doubleFactory = new DoubleObjectFactory();
		ObjectFactory<ByteBuffer> rawFactory = new ByteBufferObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();

		// XStream serializer
		XStreamMarshaller xstream = new XStreamMarshaller();
		try {
			xstream.afterPropertiesSet();
		} catch (Exception ex) {
			throw new RuntimeException("Cannot init XStream", ex);
		}

		LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory(standaloneConfiguration,
				clientConfiguration);
		lettuceConnectionFactory.afterPropertiesSet();

		LettuceConnectionFactory poolingConnectionFactory = new LettuceConnectionFactory(standaloneConfiguration,
				poolingConfiguration);
		poolingConnectionFactory.afterPropertiesSet();


		ReactiveRedisTemplate<Object, Object> objectTemplate = new ReactiveRedisTemplate<>(lettuceConnectionFactory,
				RedisSerializationContext.java(ReactiveOperationsTestParams.class.getClassLoader()));

		ReactiveRedisTemplate<Object, Object> pooledObjectTemplate = new ReactiveRedisTemplate<>(poolingConnectionFactory,
				RedisSerializationContext.java());

		StringRedisSerializer stringRedisSerializer = StringRedisSerializer.UTF_8;
		ReactiveRedisTemplate<String, String> stringTemplate = new ReactiveRedisTemplate<>(lettuceConnectionFactory,
				RedisSerializationContext.fromSerializer(stringRedisSerializer));

		JdkSerializationRedisSerializer jdkSerializationRedisSerializer = new JdkSerializationRedisSerializer();
		GenericToStringSerializer<Long> longToStringSerializer = new GenericToStringSerializer(Long.class);
		ReactiveRedisTemplate<String, Long> longTemplate = new ReactiveRedisTemplate<>(lettuceConnectionFactory,
				RedisSerializationContext.<String, Long> newSerializationContext(jdkSerializationRedisSerializer)
						.key(stringRedisSerializer).value(longToStringSerializer).build());

		GenericToStringSerializer<Double> doubleToStringSerializer = new GenericToStringSerializer(Double.class);
		ReactiveRedisTemplate<String, Double> doubleTemplate = new ReactiveRedisTemplate<>(lettuceConnectionFactory,
				RedisSerializationContext.<String, Double> newSerializationContext(jdkSerializationRedisSerializer)
						.key(stringRedisSerializer).value(doubleToStringSerializer).build());

		ReactiveRedisTemplate<ByteBuffer, ByteBuffer> rawTemplate = new ReactiveRedisTemplate<>(lettuceConnectionFactory,
				RedisSerializationContext.raw());

		ReactiveRedisTemplate<String, Person> personTemplate = new ReactiveRedisTemplate(lettuceConnectionFactory,
				RedisSerializationContext.fromSerializer(jdkSerializationRedisSerializer));

		OxmSerializer oxmSerializer = new OxmSerializer(xstream, xstream);
		ReactiveRedisTemplate<String, String> xstreamStringTemplate = new ReactiveRedisTemplate(lettuceConnectionFactory,
				RedisSerializationContext.fromSerializer(oxmSerializer));

		ReactiveRedisTemplate<String, Person> xstreamPersonTemplate = new ReactiveRedisTemplate(lettuceConnectionFactory,
				RedisSerializationContext.fromSerializer(oxmSerializer));

		Jackson2JsonRedisSerializer<Person> jackson2JsonSerializer = new Jackson2JsonRedisSerializer<>(Person.class);
		ReactiveRedisTemplate<String, Person> jackson2JsonPersonTemplate = new ReactiveRedisTemplate(
				lettuceConnectionFactory, RedisSerializationContext.fromSerializer(jackson2JsonSerializer));

		GenericJackson2JsonRedisSerializer genericJackson2JsonSerializer = new GenericJackson2JsonRedisSerializer();
		ReactiveRedisTemplate<String, Person> genericJackson2JsonPersonTemplate = new ReactiveRedisTemplate(
				lettuceConnectionFactory, RedisSerializationContext.fromSerializer(genericJackson2JsonSerializer));

		List<Object[]> list = Arrays.asList(new Object[][] { //
				{ stringTemplate, stringFactory, stringFactory, stringRedisSerializer, "String" }, //
				{ objectTemplate, personFactory, personFactory, jdkSerializationRedisSerializer, "Person/JDK" }, //
				{ pooledObjectTemplate, personFactory, personFactory, jdkSerializationRedisSerializer, "Pooled/Person/JDK" }, //
				{ longTemplate, stringFactory, longFactory, longToStringSerializer, "Long" }, //
				{ doubleTemplate, stringFactory, doubleFactory, doubleToStringSerializer, "Double" }, //
				{ rawTemplate, rawFactory, rawFactory, null, "raw" }, //
				{ personTemplate, stringFactory, personFactory, jdkSerializationRedisSerializer, "String/Person/JDK" }, //
				{ xstreamStringTemplate, stringFactory, stringFactory, oxmSerializer, "String/OXM" }, //
				{ xstreamPersonTemplate, stringFactory, personFactory, oxmSerializer, "String/Person/OXM" }, //
				{ jackson2JsonPersonTemplate, stringFactory, personFactory, jackson2JsonSerializer, "Jackson2" }, //
				{ genericJackson2JsonPersonTemplate, stringFactory, personFactory, genericJackson2JsonSerializer,
						"Generic Jackson 2" } });

		if (clusterAvailable()) {

			RedisClusterConfiguration clusterConfiguration = new RedisClusterConfiguration();
			clusterConfiguration.addClusterNode(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_1_PORT));

			ReactiveRedisTemplate<String, String> clusterStringTemplate = null;

			LettuceConnectionFactory lettuceClusterConnectionFactory = new LettuceConnectionFactory(clusterConfiguration,
					clientConfiguration);
			lettuceClusterConnectionFactory.afterPropertiesSet();

			clusterStringTemplate = new ReactiveRedisTemplate<>(lettuceClusterConnectionFactory,
					RedisSerializationContext.string());

			list = new ArrayList<>(list);
			list.add(new Object[] { clusterStringTemplate, clusterKeyStringFactory, stringFactory, stringRedisSerializer,
					"Cluster String" });
		}

		return list;
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
