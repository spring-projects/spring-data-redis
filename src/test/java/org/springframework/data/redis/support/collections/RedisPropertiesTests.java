/*
 * Copyright 2011-2019 the original author or authors.
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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import org.springframework.data.redis.DoubleAsStringObjectFactory;
import org.springframework.data.redis.LongAsStringObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientResources;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.oxm.xstream.XStreamMarshaller;

/**
 * @author Costin Leau
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class RedisPropertiesTests extends RedisMapTests {

	protected Properties defaults = new Properties();
	protected RedisProperties props;

	/**
	 * Constructs a new <code>RedisPropertiesTests</code> instance.
	 *
	 * @param keyFactory
	 * @param valueFactory
	 * @param template
	 */
	public RedisPropertiesTests(ObjectFactory<Object> keyFactory, ObjectFactory<Object> valueFactory,
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
	public void testPropertiesLoad() throws Exception {
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
	@Ignore
	public void testPropertiesLoadXml() throws Exception {
		InputStream stream = getClass()
				.getResourceAsStream("/org/springframework/data/keyvalue/redis/support/collections/props.properties");

		assertThat(stream).isNotNull();

		int size = props.size();

		try {
			props.loadFromXML(stream);
		} finally {
			stream.close();
		}

		assertThat(props.get("foo")).isEqualTo("bar");
		assertThat(props.get("bucket")).isEqualTo("head");
		assertThat(props.get("lotus")).isEqualTo("island");
		assertThat(props.size()).isEqualTo(size + 3);
	}

	@Test
	public void testPropertiesSave() throws Exception {
		props.setProperty("x", "y");
		props.setProperty("a", "b");

		StringWriter writer = new StringWriter();
		props.store(writer, "no-comment");
	}

	@Test
	@Ignore
	public void testPropertiesSaveXml() throws Exception {
		props.setProperty("x", "y");
		props.setProperty("a", "b");
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		props.storeToXML(bos, "comment");
	}

	@Test
	public void testGetProperty() throws Exception {
		String property = props.getProperty("a");
		assertThat(property).isNull();
		defaults.put("a", "x");
		assertThat(props.getProperty("a")).isEqualTo("x");
	}

	@Test
	public void testGetPropertyDefault() throws Exception {
		assertThat(props.getProperty("a", "x")).isEqualTo("x");
	}

	@Test
	public void testSetProperty() throws Exception {
		assertThat(props.getProperty("a")).isNull();
		defaults.setProperty("a", "x");
		assertThat(props.getProperty("a")).isEqualTo("x");
	}

	@Test
	public void testPropertiesList() throws Exception {
		defaults.setProperty("a", "b");
		props.setProperty("x", "y");
		StringWriter wr = new StringWriter();
		props.list(new PrintWriter(wr));
	}

	@Test
	public void testPropertyNames() throws Exception {
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
	public void testDefaultInit() throws Exception {
		RedisProperties redisProperties = new RedisProperties("foo", template);
		redisProperties.propertyNames();
	}

	@Test
	public void testStringPropertyNames() throws Exception {
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

	@Test
	@Override
	public void testScanWorksCorrectly() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> super.testScanWorksCorrectly());
	}

	// DATAREDIS-241
	@Parameters
	public static Collection<Object[]> testParams() {

		// XStream serializer
		XStreamMarshaller xstream = new XStreamMarshaller();
		try {
			xstream.afterPropertiesSet();
		} catch (Exception ex) {
			throw new RuntimeException("Cannot init XStream", ex);
		}
		OxmSerializer serializer = new OxmSerializer(xstream, xstream);
		Jackson2JsonRedisSerializer<Person> jackson2JsonSerializer = new Jackson2JsonRedisSerializer<>(Person.class);
		Jackson2JsonRedisSerializer<String> jackson2JsonStringSerializer = new Jackson2JsonRedisSerializer<>(
				String.class);

		// create Jedis Factory
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<String> longFactory = new LongAsStringObjectFactory();
		ObjectFactory<String> doubleFactory = new DoubleAsStringObjectFactory();

		JedisConnectionFactory jedisConnFactory = new JedisConnectionFactory();
		jedisConnFactory.setUsePool(true);

		jedisConnFactory.setPort(SettingsUtils.getPort());
		jedisConnFactory.setHostName(SettingsUtils.getHost());

		jedisConnFactory.afterPropertiesSet();

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

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = new LettuceConnectionFactory();
		lettuceConnFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		lettuceConnFactory.setPort(SettingsUtils.getPort());
		lettuceConnFactory.setHostName(SettingsUtils.getHost());
		lettuceConnFactory.afterPropertiesSet();

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
