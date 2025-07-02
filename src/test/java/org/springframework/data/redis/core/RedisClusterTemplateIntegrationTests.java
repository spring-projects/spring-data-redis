/*
 * Copyright 2015-2025 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assumptions.*;

import java.util.Arrays;
import java.util.Collection;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.runners.Parameterized.Parameters;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.LongObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;
import org.springframework.data.redis.RawObjectFactory;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.test.XstreamOxmSerializerSingleton;
import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.data.redis.test.extension.RedisCluster;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ParameterizedClass
@EnabledOnRedisClusterAvailable
public class RedisClusterTemplateIntegrationTests<K, V> extends RedisTemplateIntegrationTests<K, V> {

	public RedisClusterTemplateIntegrationTests(RedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory) {
		super(redisTemplate, keyFactory, valueFactory);
	}

	@Test
	@Disabled("Pipeline not supported in cluster mode")
	public void testExecutePipelinedNonNullRedisCallback() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(super::testExecutePipelinedNonNullRedisCallback);
	}

	@Test
	@Disabled("Pipeline not supported in cluster mode")
	public void testExecutePipelinedTx() {
		super.testExecutePipelinedTx();
	}

	@Test
	@Disabled("Watch only supported on same connection...")
	public void testWatch() {
		super.testWatch();
	}

	@Test
	@Disabled("Watch only supported on same connection...")
	public void testUnwatch() {
		super.testUnwatch();
	}

	@Test
	@Disabled("EXEC only supported on same connection...")
	public void testExec() {
		super.testExec();
	}

	@Test
	@Disabled("Pipeline not supported in cluster mode")
	public void testExecutePipelinedNonNullSessionCallback() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(super::testExecutePipelinedNonNullSessionCallback);
	}

	@Test
	@Disabled("PubSub not supported in cluster mode")
	public void testConvertAndSend() {
		super.testConvertAndSend();
	}

	@Test
	@Disabled("Watch only supported on same connection...")
	public void testExecConversionDisabled() {
		super.testExecConversionDisabled();
	}

	@Test
	@Disabled("Discard only supported on same connection...")
	public void testDiscard() {
		super.testDiscard();
	}

	@Test
	@Disabled("Pipleline not supported in cluster mode")
	public void testExecutePipelined() {
		super.testExecutePipelined();
	}

	@Test
	@Disabled("Watch only supported on same connection...")
	public void testWatchMultipleKeys() {
		super.testWatchMultipleKeys();
	}

	@Test
	@Disabled("This one fails when using GET options on numbers")
	public void testSortBulkMapper() {
		super.testSortBulkMapper();
	}

	@Test
	@Disabled("This one fails when using GET options on numbers")
	public void testGetExpireMillisUsingTransactions() {
		super.testGetExpireMillisUsingTransactions();
	}

	@Test
	@Disabled("This one fails when using GET options on numbers")
	public void testGetExpireMillisUsingPipelining() {
		super.testGetExpireMillisUsingPipelining();
	}

	@Test
	void testScan() {

		// Only Lettuce supports cluster-wide scanning
		assumeThat(redisTemplate.getConnectionFactory()).isInstanceOf(LettuceConnectionFactory.class);
		super.testScan();
	}

	@Parameters
	public static Collection<Object[]> testParams() {

		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<Long> longFactory = new LongObjectFactory();
		ObjectFactory<byte[]> rawFactory = new RawObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();


		OxmSerializer serializer = XstreamOxmSerializerSingleton.getInstance();
		Jackson2JsonRedisSerializer<Person> jackson2JsonSerializer = new Jackson2JsonRedisSerializer<>(Person.class);

		// JEDIS
		JedisConnectionFactory jedisConnectionFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(RedisCluster.class);

		jedisConnectionFactory.afterPropertiesSet();

		RedisTemplate<String, String> jedisStringTemplate = new RedisTemplate<>();
		jedisStringTemplate.setDefaultSerializer(StringRedisSerializer.UTF_8);
		jedisStringTemplate.setConnectionFactory(jedisConnectionFactory);
		jedisStringTemplate.afterPropertiesSet();

		RedisTemplate<String, Long> jedisLongTemplate = new RedisTemplate<>();
		jedisLongTemplate.setKeySerializer(StringRedisSerializer.UTF_8);
		jedisLongTemplate.setValueSerializer(new GenericToStringSerializer<>(Long.class));
		jedisLongTemplate.setConnectionFactory(jedisConnectionFactory);
		jedisLongTemplate.afterPropertiesSet();

		RedisTemplate<byte[], byte[]> jedisRawTemplate = new RedisTemplate<>();
		jedisRawTemplate.setEnableDefaultSerializer(false);
		jedisRawTemplate.setConnectionFactory(jedisConnectionFactory);
		jedisRawTemplate.afterPropertiesSet();

		RedisTemplate<String, Person> jedisPersonTemplate = new RedisTemplate<>();
		jedisPersonTemplate.setConnectionFactory(jedisConnectionFactory);
		jedisPersonTemplate.afterPropertiesSet();

		RedisTemplate<String, String> jedisXstreamStringTemplate = new RedisTemplate<>();
		jedisXstreamStringTemplate.setConnectionFactory(jedisConnectionFactory);
		jedisXstreamStringTemplate.setDefaultSerializer(serializer);
		jedisXstreamStringTemplate.afterPropertiesSet();

		RedisTemplate<String, Person> jedisJackson2JsonPersonTemplate = new RedisTemplate<>();
		jedisJackson2JsonPersonTemplate.setConnectionFactory(jedisConnectionFactory);
		jedisJackson2JsonPersonTemplate.setValueSerializer(jackson2JsonSerializer);
		jedisJackson2JsonPersonTemplate.afterPropertiesSet();

		// LETTUCE

		LettuceConnectionFactory lettuceConnectionFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(RedisCluster.class);

		RedisTemplate<String, String> lettuceStringTemplate = new RedisTemplate<>();
		lettuceStringTemplate.setDefaultSerializer(StringRedisSerializer.UTF_8);
		lettuceStringTemplate.setConnectionFactory(lettuceConnectionFactory);
		lettuceStringTemplate.afterPropertiesSet();

		RedisTemplate<String, Long> lettuceLongTemplate = new RedisTemplate<>();
		lettuceLongTemplate.setKeySerializer(StringRedisSerializer.UTF_8);
		lettuceLongTemplate.setValueSerializer(new GenericToStringSerializer<>(Long.class));
		lettuceLongTemplate.setConnectionFactory(lettuceConnectionFactory);
		lettuceLongTemplate.afterPropertiesSet();

		RedisTemplate<byte[], byte[]> lettuceRawTemplate = new RedisTemplate<>();
		lettuceRawTemplate.setEnableDefaultSerializer(false);
		lettuceRawTemplate.setConnectionFactory(lettuceConnectionFactory);
		lettuceRawTemplate.afterPropertiesSet();

		RedisTemplate<String, Person> lettucePersonTemplate = new RedisTemplate<>();
		lettucePersonTemplate.setConnectionFactory(lettuceConnectionFactory);
		lettucePersonTemplate.afterPropertiesSet();

		RedisTemplate<String, String> lettuceXstreamStringTemplate = new RedisTemplate<>();
		lettuceXstreamStringTemplate.setConnectionFactory(lettuceConnectionFactory);
		lettuceXstreamStringTemplate.setDefaultSerializer(serializer);
		lettuceXstreamStringTemplate.afterPropertiesSet();

		RedisTemplate<String, Person> lettuceJackson2JsonPersonTemplate = new RedisTemplate<>();
		lettuceJackson2JsonPersonTemplate.setConnectionFactory(lettuceConnectionFactory);
		lettuceJackson2JsonPersonTemplate.setValueSerializer(jackson2JsonSerializer);
		lettuceJackson2JsonPersonTemplate.afterPropertiesSet();

		return Arrays.asList(new Object[][] { //

				// JEDIS
				{ jedisStringTemplate, stringFactory, stringFactory }, //
				{ jedisLongTemplate, stringFactory, longFactory }, //
				{ jedisRawTemplate, rawFactory, rawFactory }, //
				{ jedisPersonTemplate, stringFactory, personFactory }, //
				{ jedisXstreamStringTemplate, stringFactory, stringFactory }, //
				{ jedisJackson2JsonPersonTemplate, stringFactory, personFactory }, //

				// LETTUCE
				{ lettuceStringTemplate, stringFactory, stringFactory }, //
				{ lettuceLongTemplate, stringFactory, longFactory }, //
				{ lettuceRawTemplate, rawFactory, rawFactory }, //
				{ lettucePersonTemplate, stringFactory, personFactory }, //
				{ lettuceXstreamStringTemplate, stringFactory, stringFactory }, //
				{ lettuceJackson2JsonPersonTemplate, stringFactory, personFactory }, //
		});
	}

}
