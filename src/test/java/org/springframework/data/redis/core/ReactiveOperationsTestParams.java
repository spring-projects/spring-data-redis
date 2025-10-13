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
package org.springframework.data.redis.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.ByteBufferObjectFactory;
import org.springframework.data.redis.DoubleObjectFactory;
import org.springframework.data.redis.LongObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;
import org.springframework.data.redis.PrefixStringObjectFactory;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.test.XstreamOxmSerializerSingleton;
import org.springframework.data.redis.test.condition.RedisDetector;
import org.springframework.data.redis.test.extension.RedisCluster;
import org.springframework.data.redis.test.extension.RedisStandalone;

/**
 * Parameters for testing implementations of {@link ReactiveRedisTemplate}
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
abstract public class ReactiveOperationsTestParams {

	public static Collection<Fixture<?, ?>> testParams() {

		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<String> clusterKeyStringFactory = new PrefixStringObjectFactory("{u1}.", stringFactory);
		ObjectFactory<Long> longFactory = new LongObjectFactory();
		ObjectFactory<Double> doubleFactory = new DoubleObjectFactory();
		ObjectFactory<ByteBuffer> rawFactory = new ByteBufferObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();

		LettuceConnectionFactory lettuceConnectionFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(RedisStandalone.class);

		ReactiveRedisTemplate<Object, Object> objectTemplate = new ReactiveRedisTemplate<>(lettuceConnectionFactory,
				RedisSerializationContext.java(ReactiveOperationsTestParams.class.getClassLoader()));

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
				RedisSerializationContext.byteBuffer());

		ReactiveRedisTemplate<String, Person> personTemplate = new ReactiveRedisTemplate(lettuceConnectionFactory,
				RedisSerializationContext.fromSerializer(jdkSerializationRedisSerializer));

		OxmSerializer oxmSerializer = XstreamOxmSerializerSingleton.getInstance();
		ReactiveRedisTemplate<String, String> xstreamStringTemplate = new ReactiveRedisTemplate(lettuceConnectionFactory,
				RedisSerializationContext.fromSerializer(oxmSerializer));

		ReactiveRedisTemplate<String, Person> xstreamPersonTemplate = new ReactiveRedisTemplate(lettuceConnectionFactory,
				RedisSerializationContext.fromSerializer(oxmSerializer));

		Jackson2JsonRedisSerializer<Person> jackson2JsonSerializer = new Jackson2JsonRedisSerializer<>(Person.class);
		ReactiveRedisTemplate<String, Person> jackson2JsonPersonTemplate = new ReactiveRedisTemplate(
				lettuceConnectionFactory, RedisSerializationContext.fromSerializer(jackson2JsonSerializer));

		JacksonJsonRedisSerializer<Person> jackson3JsonSerializer = new JacksonJsonRedisSerializer<>(Person.class);
		ReactiveRedisTemplate<String, Person> jackson3JsonPersonTemplate = new ReactiveRedisTemplate(
			lettuceConnectionFactory, RedisSerializationContext.fromSerializer(jackson3JsonSerializer));

		GenericJackson2JsonRedisSerializer genericJackson2JsonSerializer = new GenericJackson2JsonRedisSerializer();
		ReactiveRedisTemplate<String, Person> genericJackson2JsonPersonTemplate = new ReactiveRedisTemplate(
				lettuceConnectionFactory, RedisSerializationContext.fromSerializer(genericJackson2JsonSerializer));

		GenericJacksonJsonRedisSerializer genericJacksonJsonSerializer = GenericJacksonJsonRedisSerializer
            .create(it -> it.enableSpringCacheNullValueSupport().enableUnsafeDefaultTyping());
		ReactiveRedisTemplate<String, Person> genericJacksonJsonPersonTemplate = new ReactiveRedisTemplate(
				lettuceConnectionFactory, RedisSerializationContext.fromSerializer(genericJacksonJsonSerializer));

		List<Fixture<?, ?>> list = Arrays.asList( //
				new Fixture<>(stringTemplate, stringFactory, stringFactory, stringRedisSerializer, "String"), //
				new Fixture<>(objectTemplate, personFactory, personFactory, jdkSerializationRedisSerializer, "Person/JDK"), //
				new Fixture<>(longTemplate, stringFactory, longFactory, longToStringSerializer, "Long"), //
				new Fixture<>(doubleTemplate, stringFactory, doubleFactory, doubleToStringSerializer, "Double"), //
				new Fixture<>(rawTemplate, rawFactory, rawFactory, null, "raw"), //
				new Fixture<>(personTemplate, stringFactory, personFactory, jdkSerializationRedisSerializer,
						"String/Person/JDK"), //
				new Fixture<>(xstreamStringTemplate, stringFactory, stringFactory, oxmSerializer, "String/OXM"), //
				new Fixture<>(xstreamPersonTemplate, stringFactory, personFactory, oxmSerializer, "String/Person/OXM"), //
				new Fixture<>(jackson2JsonPersonTemplate, stringFactory, personFactory, jackson2JsonSerializer, "Jackson2"), //
				new Fixture<>(jackson3JsonPersonTemplate, stringFactory, personFactory, jackson2JsonSerializer, "Jackson3"), //
				new Fixture<>(genericJackson2JsonPersonTemplate, stringFactory, personFactory, genericJackson2JsonSerializer,
						"Generic Jackson 2"),
				new Fixture<>(genericJacksonJsonPersonTemplate, stringFactory, personFactory, genericJacksonJsonSerializer,
						"Generic Jackson 3"));

		if (clusterAvailable()) {

			ReactiveRedisTemplate<String, String> clusterStringTemplate;

			LettuceConnectionFactory lettuceClusterConnectionFactory = LettuceConnectionFactoryExtension
					.getConnectionFactory(RedisCluster.class);

			clusterStringTemplate = new ReactiveRedisTemplate<>(lettuceClusterConnectionFactory,
					RedisSerializationContext.string());

			list = new ArrayList<>(list);
			list.add(new Fixture<>(clusterStringTemplate, clusterKeyStringFactory, stringFactory, stringRedisSerializer,
					"Cluster String"));
		}

		return list;
	}

	private static boolean clusterAvailable() {
		return RedisDetector.isClusterAvailable();
	}

	static class Fixture<K, V> {

		private final ReactiveRedisTemplate<K, V> template;
		private final ObjectFactory<K> keyFactory;
		private final ObjectFactory<V> valueFactory;
		private final @Nullable RedisSerializer<K> serializer;
		private final String label;

		public Fixture(ReactiveRedisTemplate<?, ?> template, ObjectFactory<K> keyFactory, ObjectFactory<V> valueFactory,
				@Nullable RedisSerializer<?> serializer, String label) {

			this.template = (ReactiveRedisTemplate) template;
			this.keyFactory = keyFactory;
			this.valueFactory = valueFactory;
			this.serializer = (RedisSerializer) serializer;
			this.label = label;
		}

		public ReactiveRedisTemplate<K, V> getTemplate() {
			return template;
		}

		public ObjectFactory<K> getKeyFactory() {
			return keyFactory;
		}

		public ObjectFactory<V> getValueFactory() {
			return valueFactory;
		}

		public @Nullable RedisSerializer getSerializer() {
			return serializer;
		}

		public String getLabel() {
			return label;
		}

		@Override
		public String toString() {
			return label;
		}
	}

}
