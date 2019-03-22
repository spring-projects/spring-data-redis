/*
 * Copyright 2011-2017 the original author or authors.
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
import org.springframework.data.redis.connection.jredis.JredisConnectionFactory;
import org.springframework.data.redis.connection.jredis.JredisPool;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientResources;
import org.springframework.data.redis.connection.srp.SrpConnectionFactory;
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
		assertThat(map.getOperations() instanceof StringRedisTemplate).isTrue();
	}

	@Test
	public void testPropertiesLoad() throws Exception {
		InputStream stream = getClass().getResourceAsStream(
				"/org/springframework/data/redis/support/collections/props.properties");

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
		InputStream stream = getClass().getResourceAsStream(
				"/org/springframework/data/keyvalue/redis/support/collections/props.properties");

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
		System.out.println(bos.toString());
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
		Set<Object> keys = new LinkedHashSet<Object>();
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

	@Override
	@Test(expected = UnsupportedOperationException.class)
	public void testScanWorksCorrectly() throws IOException {
		super.testScanWorksCorrectly();
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

		// JRedis
		JredisConnectionFactory jredisConnFactory = new JredisConnectionFactory(new JredisPool(SettingsUtils.getHost(),
				SettingsUtils.getPort()));
		jredisConnFactory.afterPropertiesSet();

		RedisTemplate<String, String> genericTemplateJR = new StringRedisTemplate(jredisConnFactory);
		RedisTemplate<String, Person> xGenericTemplateJR = new RedisTemplate<String, Person>();
		xGenericTemplateJR.setConnectionFactory(jredisConnFactory);
		xGenericTemplateJR.setDefaultSerializer(serializer);
		xGenericTemplateJR.afterPropertiesSet();

		RedisTemplate<String, Person> jsonPersonTemplateJR = new RedisTemplate<String, Person>();
		jsonPersonTemplateJR.setConnectionFactory(jredisConnFactory);
		jsonPersonTemplateJR.setDefaultSerializer(jsonSerializer);
		jsonPersonTemplateJR.setHashKeySerializer(jsonSerializer);
		jsonPersonTemplateJR.setHashValueSerializer(jsonStringSerializer);
		jsonPersonTemplateJR.afterPropertiesSet();

		RedisTemplate<String, Person> jackson2JsonPersonTemplateJR = new RedisTemplate<String, Person>();
		jackson2JsonPersonTemplateJR.setConnectionFactory(jredisConnFactory);
		jackson2JsonPersonTemplateJR.setDefaultSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplateJR.setHashKeySerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplateJR.setHashValueSerializer(jackson2JsonStringSerializer);
		jackson2JsonPersonTemplateJR.afterPropertiesSet();

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

		// SRP
		SrpConnectionFactory srpConnFactory = new SrpConnectionFactory();
		srpConnFactory.setPort(SettingsUtils.getPort());
		srpConnFactory.setHostName(SettingsUtils.getHost());
		srpConnFactory.afterPropertiesSet();

		RedisTemplate<String, String> genericTemplateSrp = new StringRedisTemplate(srpConnFactory);
		RedisTemplate<String, Person> xGenericTemplateSrp = new RedisTemplate<String, Person>();
		xGenericTemplateSrp.setConnectionFactory(srpConnFactory);
		xGenericTemplateSrp.setDefaultSerializer(serializer);
		xGenericTemplateSrp.afterPropertiesSet();

		RedisTemplate<String, Person> jsonPersonTemplateSrp = new RedisTemplate<String, Person>();
		jsonPersonTemplateSrp.setConnectionFactory(srpConnFactory);
		jsonPersonTemplateSrp.setDefaultSerializer(jsonSerializer);
		jsonPersonTemplateSrp.setHashKeySerializer(jsonSerializer);
		jsonPersonTemplateSrp.setHashValueSerializer(jsonStringSerializer);
		jsonPersonTemplateSrp.afterPropertiesSet();

		RedisTemplate<String, Person> jackson2JsonPersonTemplateSrp = new RedisTemplate<String, Person>();
		jackson2JsonPersonTemplateSrp.setConnectionFactory(srpConnFactory);
		jackson2JsonPersonTemplateSrp.setDefaultSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplateSrp.setHashKeySerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplateSrp.setHashValueSerializer(jackson2JsonStringSerializer);
		jackson2JsonPersonTemplateSrp.afterPropertiesSet();

		return Arrays.asList(new Object[][] { { stringFactory, stringFactory, genericTemplate },
				{ stringFactory, stringFactory, genericTemplate }, { stringFactory, stringFactory, genericTemplate },
				{ stringFactory, stringFactory, genericTemplate }, { stringFactory, stringFactory, xstreamGenericTemplate },
				{ stringFactory, stringFactory, genericTemplateJR }, { stringFactory, stringFactory, genericTemplateJR },
				{ stringFactory, stringFactory, genericTemplateJR }, { stringFactory, stringFactory, genericTemplateJR },
				{ stringFactory, stringFactory, xGenericTemplateJR }, { stringFactory, stringFactory, jsonPersonTemplate },
				{ stringFactory, stringFactory, jackson2JsonPersonTemplate },
				{ stringFactory, stringFactory, jsonPersonTemplateJR },
				{ stringFactory, stringFactory, jackson2JsonPersonTemplateJR },
				{ stringFactory, stringFactory, genericTemplateLtc }, { stringFactory, stringFactory, genericTemplateLtc },
				{ stringFactory, stringFactory, genericTemplateLtc }, { stringFactory, stringFactory, genericTemplateLtc },
				{ stringFactory, doubleFactory, genericTemplateLtc }, { stringFactory, longFactory, genericTemplateLtc },
				{ stringFactory, stringFactory, xGenericTemplateLtc }, { stringFactory, stringFactory, jsonPersonTemplateLtc },
				{ stringFactory, stringFactory, jackson2JsonPersonTemplateLtc },
				{ stringFactory, stringFactory, genericTemplateSrp }, { stringFactory, stringFactory, genericTemplateSrp },
				{ stringFactory, stringFactory, genericTemplateSrp }, { stringFactory, stringFactory, genericTemplateSrp },
				{ stringFactory, doubleFactory, genericTemplateSrp }, { stringFactory, longFactory, genericTemplateSrp },
				{ stringFactory, stringFactory, xGenericTemplateSrp }, { stringFactory, stringFactory, jsonPersonTemplateSrp },
				{ stringFactory, stringFactory, jackson2JsonPersonTemplateSrp } });
	}

}
