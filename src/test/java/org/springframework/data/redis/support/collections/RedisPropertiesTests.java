/*
 * Copyright 2011-2016 the original author or authors.
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
package org.springframework.data.redis.support.collections;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
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
		assertTrue(map.getOperations() instanceof StringRedisTemplate);
	}

	@Test
	public void testPropertiesLoad() throws Exception {
		InputStream stream = getClass()
				.getResourceAsStream("/org/springframework/data/redis/support/collections/props.properties");

		assertNotNull(stream);

		int size = props.size();

		try {
			props.load(stream);
		} finally {
			stream.close();
		}

		assertEquals("bar", props.get("foo"));
		assertEquals("head", props.get("bucket"));
		assertEquals("island", props.get("lotus"));
		assertEquals(size + 3, props.size());
	}

	@Test
	@Ignore
	public void testPropertiesLoadXml() throws Exception {
		InputStream stream = getClass()
				.getResourceAsStream("/org/springframework/data/keyvalue/redis/support/collections/props.properties");

		assertNotNull(stream);

		int size = props.size();

		try {
			props.loadFromXML(stream);
		} finally {
			stream.close();
		}

		assertEquals("bar", props.get("foo"));
		assertEquals("head", props.get("bucket"));
		assertEquals("island", props.get("lotus"));
		assertEquals(size + 3, props.size());
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
		System.out.println(bos.toString());
	}

	@Test
	public void testGetProperty() throws Exception {
		String property = props.getProperty("a");
		assertNull(property);
		defaults.put("a", "x");
		assertEquals("x", props.getProperty("a"));
	}

	@Test
	public void testGetPropertyDefault() throws Exception {
		assertEquals("x", props.getProperty("a", "x"));
	}

	@Test
	public void testSetProperty() throws Exception {
		assertNull(props.getProperty("a"));
		defaults.setProperty("a", "x");
		assertEquals("x", props.getProperty("a"));
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
		Set<Object> keys = new LinkedHashSet<Object>();
		keys.add(names.nextElement());
		keys.add(names.nextElement());
		keys.add(names.nextElement());

		assertFalse(names.hasMoreElements());
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
		assertTrue(keys.contains(key1));
		assertTrue(keys.contains(key2));
		assertTrue(keys.contains(key3));
	}

	@Override
	@Test(expected = UnsupportedOperationException.class)
	public void testScanWorksCorrectly() throws IOException {
		super.testScanWorksCorrectly();
	}

	/**
	 * @see DATAREDIS-241
	 */
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
		JacksonJsonRedisSerializer<Person> jsonSerializer = new JacksonJsonRedisSerializer<Person>(Person.class);
		JacksonJsonRedisSerializer<String> jsonStringSerializer = new JacksonJsonRedisSerializer<String>(String.class);
		Jackson2JsonRedisSerializer<Person> jackson2JsonSerializer = new Jackson2JsonRedisSerializer<Person>(Person.class);
		Jackson2JsonRedisSerializer<String> jackson2JsonStringSerializer = new Jackson2JsonRedisSerializer<String>(
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

		RedisTemplate<String, String> xstreamGenericTemplate = new RedisTemplate<String, String>();
		xstreamGenericTemplate.setConnectionFactory(jedisConnFactory);
		xstreamGenericTemplate.setDefaultSerializer(serializer);
		xstreamGenericTemplate.afterPropertiesSet();

		RedisTemplate<String, Person> jsonPersonTemplate = new RedisTemplate<String, Person>();
		jsonPersonTemplate.setConnectionFactory(jedisConnFactory);
		jsonPersonTemplate.setDefaultSerializer(jsonSerializer);
		jsonPersonTemplate.setHashKeySerializer(jsonSerializer);
		jsonPersonTemplate.setHashValueSerializer(jsonStringSerializer);
		jsonPersonTemplate.afterPropertiesSet();

		RedisTemplate<String, Person> jackson2JsonPersonTemplate = new RedisTemplate<String, Person>();
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
		RedisTemplate<String, Person> xGenericTemplateLtc = new RedisTemplate<String, Person>();
		xGenericTemplateLtc.setConnectionFactory(lettuceConnFactory);
		xGenericTemplateLtc.setDefaultSerializer(serializer);
		xGenericTemplateLtc.afterPropertiesSet();

		RedisTemplate<String, Person> jsonPersonTemplateLtc = new RedisTemplate<String, Person>();
		jsonPersonTemplateLtc.setConnectionFactory(lettuceConnFactory);
		jsonPersonTemplateLtc.setDefaultSerializer(jsonSerializer);
		jsonPersonTemplateLtc.setHashKeySerializer(jsonSerializer);
		jsonPersonTemplateLtc.setHashValueSerializer(jsonStringSerializer);
		jsonPersonTemplateLtc.afterPropertiesSet();

		RedisTemplate<String, Person> jackson2JsonPersonTemplateLtc = new RedisTemplate<String, Person>();
		jackson2JsonPersonTemplateLtc.setConnectionFactory(lettuceConnFactory);
		jackson2JsonPersonTemplateLtc.setDefaultSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplateLtc.setHashKeySerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplateLtc.setHashValueSerializer(jackson2JsonStringSerializer);
		jackson2JsonPersonTemplateLtc.afterPropertiesSet();

		return Arrays.asList(new Object[][] { { stringFactory, stringFactory, genericTemplate },
				{ stringFactory, stringFactory, genericTemplate }, { stringFactory, stringFactory, genericTemplate },
				{ stringFactory, stringFactory, genericTemplate }, { stringFactory, stringFactory, xstreamGenericTemplate },
				{ stringFactory, stringFactory, jsonPersonTemplate },
				{ stringFactory, stringFactory, jackson2JsonPersonTemplate },

				{ stringFactory, stringFactory, genericTemplateLtc }, { stringFactory, stringFactory, genericTemplateLtc },
				{ stringFactory, stringFactory, genericTemplateLtc }, { stringFactory, stringFactory, genericTemplateLtc },
				{ stringFactory, doubleFactory, genericTemplateLtc }, { stringFactory, longFactory, genericTemplateLtc },
				{ stringFactory, stringFactory, xGenericTemplateLtc }, { stringFactory, stringFactory, jsonPersonTemplateLtc },
				{ stringFactory, stringFactory, jackson2JsonPersonTemplateLtc } });
	}

}
