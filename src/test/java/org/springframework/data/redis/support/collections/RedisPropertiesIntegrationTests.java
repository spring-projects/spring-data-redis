/*
 * Copyright 2011-2025 the original author or authors.
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
package org.springframework.data.redis.support.collections;

import static org.assertj.core.api.Assertions.*;

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;

import org.springframework.data.redis.DoubleAsStringObjectFactory;
import org.springframework.data.redis.LongAsStringObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.data.redis.test.XstreamOxmSerializerSingleton;
import org.springframework.data.redis.test.extension.RedisStandalone;

/**
 * @author Costin Leau
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ParameterizedClass
public class RedisPropertiesIntegrationTests extends RedisMapIntegrationTests {

	private Properties defaults = new Properties();
	private RedisProperties props;

	/**
	 * Constructs a new <code>RedisPropertiesTests</code> instance.
	 *
	 * @param keyFactory
	 * @param valueFactory
	 * @param template
	 */
	public RedisPropertiesIntegrationTests(ObjectFactory<Object> keyFactory, ObjectFactory<Object> valueFactory,
			RedisTemplate template) {
		super(keyFactory, valueFactory, template);
	}

	RedisMap<Object, Object> createMap() {
		String redisName = getClass().getSimpleName();
		props = new RedisProperties(defaults, redisName, new StringRedisTemplate(template.getConnectionFactory()));
		return props;
	}

	protected RedisStore copyStore(RedisStore store) {
		return new RedisProperties(store.getKey(), store.getOperations());
	}

	@Test
	public void testGetOperations() {
		assertThat(map.getOperations() instanceof StringRedisTemplate).isTrue();
	}

	@Test
	void testPropertiesLoad() throws Exception {
		InputStream stream = getClass()
				.getResourceAsStream("/org/springframework/data/redis/support/collections/props.properties");

		assertThat(stream).isNotNull();

		int size = props.size();

		try {
			props.load(stream);
		} finally {
			stream.close();
		}

		assertThat(props.get("foo")).isEqualTo("bar");
		assertThat(props.get("bucket")).isEqualTo("head");
		assertThat(props.get("lotus")).isEqualTo("island");
		assertThat(props.size()).isEqualTo(size + 3);
	}

	@Test
	void testPropertiesSave() throws Exception {
		props.setProperty("x", "y");
		props.setProperty("a", "b");

		StringWriter writer = new StringWriter();
		props.store(writer, "no-comment");
	}

	@Test
	void testGetProperty() throws Exception {
		String property = props.getProperty("a");
		assertThat(property).isNull();
		defaults.put("a", "x");
		assertThat(props.getProperty("a")).isEqualTo("x");
	}

	@Test
	void testGetPropertyDefault() throws Exception {
		assertThat(props.getProperty("a", "x")).isEqualTo("x");
	}

	@Test
	void testSetProperty() throws Exception {
		assertThat(props.getProperty("a")).isNull();
		defaults.setProperty("a", "x");
		assertThat(props.getProperty("a")).isEqualTo("x");
	}

	@Test
	void testPropertiesList() throws Exception {
		defaults.setProperty("a", "b");
		props.setProperty("x", "y");
		StringWriter wr = new StringWriter();
		props.list(new PrintWriter(wr));
	}

	@Test
	void testPropertyNames() throws Exception {
		String key1 = "foo";
		String key2 = "x";
		String key3 = "d";

		String val = "o";

		defaults.setProperty(key3, val);
		props.setProperty(key1, val);
		props.setProperty(key2, val);

		Enumeration<?> names = props.propertyNames();
		Set<Object> keys = new LinkedHashSet<>();
		keys.add(names.nextElement());
		keys.add(names.nextElement());
		keys.add(names.nextElement());

		assertThat(names.hasMoreElements()).isFalse();
	}

	@Test
	void testDefaultInit() throws Exception {
		RedisProperties redisProperties = new RedisProperties("foo", template);
		redisProperties.propertyNames();
	}

	@Test
	void testStringPropertyNames() throws Exception {
		String key1 = "foo";
		String key2 = "x";
		String key3 = "d";

		String val = "o";

		defaults.setProperty(key3, val);
		props.setProperty(key1, val);
		props.setProperty(key2, val);

		Set<String> keys = props.stringPropertyNames();
		assertThat(keys.contains(key1)).isTrue();
		assertThat(keys.contains(key2)).isTrue();
		assertThat(keys.contains(key3)).isTrue();
	}

	// DATAREDIS-241
	public static Collection<Object[]> testParams() {

		OxmSerializer serializer = XstreamOxmSerializerSingleton.getInstance();
		Jackson2JsonRedisSerializer<Person> jackson2JsonSerializer = new Jackson2JsonRedisSerializer<>(Person.class);
		Jackson2JsonRedisSerializer<String> jackson2JsonStringSerializer = new Jackson2JsonRedisSerializer<>(
				String.class);
		JacksonJsonRedisSerializer<Person> jackson3JsonSerializer = new JacksonJsonRedisSerializer<>(Person.class);
		JacksonJsonRedisSerializer<String> jackson3JsonStringSerializer = new JacksonJsonRedisSerializer<>(
			String.class);

		// create Jedis Factory
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<String> longFactory = new LongAsStringObjectFactory();
		ObjectFactory<String> doubleFactory = new DoubleAsStringObjectFactory();

		JedisConnectionFactory jedisConnFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(RedisStandalone.class);

		RedisTemplate<String, String> genericTemplate = new StringRedisTemplate(jedisConnFactory);

		RedisTemplate<String, String> xstreamGenericTemplate = new RedisTemplate<>();
		xstreamGenericTemplate.setConnectionFactory(jedisConnFactory);
		xstreamGenericTemplate.setDefaultSerializer(serializer);
		xstreamGenericTemplate.afterPropertiesSet();

		RedisTemplate<String, Person> jackson2JsonPersonTemplate = new RedisTemplate<>();
		jackson2JsonPersonTemplate.setConnectionFactory(jedisConnFactory);
		jackson2JsonPersonTemplate.setDefaultSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplate.setHashKeySerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplate.setHashValueSerializer(jackson2JsonStringSerializer);
		jackson2JsonPersonTemplate.afterPropertiesSet();

		RedisTemplate<String, Person> jackson3JsonPersonTemplate = new RedisTemplate<>();
		jackson3JsonPersonTemplate.setConnectionFactory(jedisConnFactory);
		jackson3JsonPersonTemplate.setDefaultSerializer(jackson3JsonSerializer);
		jackson3JsonPersonTemplate.setHashKeySerializer(jackson3JsonSerializer);
		jackson3JsonPersonTemplate.setHashValueSerializer(jackson3JsonStringSerializer);
		jackson3JsonPersonTemplate.afterPropertiesSet();

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(RedisStandalone.class, false);

		RedisTemplate<String, String> genericTemplateLtc = new StringRedisTemplate(lettuceConnFactory);
		RedisTemplate<String, Person> xGenericTemplateLtc = new RedisTemplate<>();
		xGenericTemplateLtc.setConnectionFactory(lettuceConnFactory);
		xGenericTemplateLtc.setDefaultSerializer(serializer);
		xGenericTemplateLtc.afterPropertiesSet();

		RedisTemplate<String, Person> jackson2JsonPersonTemplateLtc = new RedisTemplate<>();
		jackson2JsonPersonTemplateLtc.setConnectionFactory(lettuceConnFactory);
		jackson2JsonPersonTemplateLtc.setDefaultSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplateLtc.setHashKeySerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplateLtc.setHashValueSerializer(jackson2JsonStringSerializer);
		jackson2JsonPersonTemplateLtc.afterPropertiesSet();

		return Arrays.asList(new Object[][] { { stringFactory, stringFactory, genericTemplate }, //
				{ stringFactory, stringFactory, genericTemplate }, //
				{ stringFactory, stringFactory, genericTemplate }, //
				{ stringFactory, stringFactory, genericTemplate }, //
				{ stringFactory, stringFactory, xstreamGenericTemplate }, //
				{ stringFactory, stringFactory, jackson2JsonPersonTemplate }, //

						// lettuce
						{ stringFactory, stringFactory, genericTemplateLtc }, //
						{ stringFactory, stringFactory, genericTemplateLtc }, //
						{ stringFactory, stringFactory, genericTemplateLtc }, //
						{ stringFactory, stringFactory, genericTemplateLtc }, //
						{ stringFactory, doubleFactory, genericTemplateLtc }, //
						{ stringFactory, longFactory, genericTemplateLtc }, //
						{ stringFactory, stringFactory, xGenericTemplateLtc }, //
						{ stringFactory, stringFactory, jackson2JsonPersonTemplateLtc } });
	}

}
