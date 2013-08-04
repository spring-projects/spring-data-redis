/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.redis.core.script;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.scripting.support.StaticScriptSource;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.annotation.ProfileValueSourceConfiguration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration test of {@link DefaultScriptExecutor}
 * 
 * @author Jennifer Hickey
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@ProfileValueSourceConfiguration(RedisTestProfileValueSource.class)
@IfProfileValue(name = "redisVersion", value = "2.6")
public class DefaultScriptExecutorTests {

	@Autowired
	private RedisConnectionFactory connFactory;

	@SuppressWarnings("rawtypes")
	private RedisTemplate template;

	@SuppressWarnings("unchecked")
	@After
	public void tearDown() {
		template.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) {
				connection.flushDb();
				connection.scriptFlush();
				return null;
			}
		});
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExecuteLongResult() {
		this.template = new StringRedisTemplate();
		template.setConnectionFactory(connFactory);
		template.afterPropertiesSet();
		RedisScript<Long> script = new DefaultRedisScript<Long>(new ClassPathResource(
				"org/springframework/data/redis/core/script/increment.lua"), Long.class);
		ScriptExecutor<String> scriptExecutor = new DefaultScriptExecutor<String>(template);
		Long result = scriptExecutor.execute(script, Collections.singletonList("mykey"));
		assertNull(result);
		template.boundValueOps("mykey").set("2");
		assertEquals(Long.valueOf(3),
				scriptExecutor.execute(script, Collections.singletonList("mykey")));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExecuteBooleanResult() {
		this.template = new RedisTemplate<String, Long>();
		template.setKeySerializer(new StringRedisSerializer());
		template.setValueSerializer(new GenericToStringSerializer<Long>(Long.class));
		template.setConnectionFactory(connFactory);
		template.afterPropertiesSet();
		RedisScript<Boolean> script = new DefaultRedisScript<Boolean>(new ClassPathResource(
				"org/springframework/data/redis/core/script/cas.lua"), Boolean.class);
		ScriptExecutor<String> scriptExecutor = new DefaultScriptExecutor<String>(template);
		template.boundValueOps("counter").set(0l);
		Boolean valueSet = scriptExecutor.execute(script, Collections.singletonList("counter"), 0,
				3);
		assertTrue(valueSet);
		assertFalse(scriptExecutor.execute(script, Collections.singletonList("counter"), 0, 3));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testExecuteListResultCustomArgsSerializer() {
		this.template = new StringRedisTemplate();
		template.setConnectionFactory(connFactory);
		template.afterPropertiesSet();
		template.boundListOps("mylist").leftPushAll("a", "b", "c", "d");
		RedisScript<List<String>> script = new DefaultRedisScript(new ClassPathResource(
				"org/springframework/data/redis/core/script/bulkpop.lua"), List.class);
		ScriptExecutor<String> scriptExecutor = new DefaultScriptExecutor<String>(template);
		List<String> result = scriptExecutor
				.execute(script, new GenericToStringSerializer<Long>(Long.class),
						template.getValueSerializer(), Collections.singletonList("mylist"), 1l);
		assertEquals(Collections.singletonList("a"), result);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testExecuteMixedListResult() {
		this.template = new StringRedisTemplate();
		template.setConnectionFactory(connFactory);
		template.afterPropertiesSet();
		RedisScript<List<Object>> script = new DefaultRedisScript(new ClassPathResource(
				"org/springframework/data/redis/core/script/popandlength.lua"), List.class);
		ScriptExecutor<String> scriptExecutor = new DefaultScriptExecutor<String>(template);
		List<Object> results = scriptExecutor.execute(script, Collections.singletonList("mylist"));
		assertEquals(Arrays.asList(new Object[] { null, 0l }), results);
		template.boundListOps("mylist").leftPushAll("a", "b");
		assertEquals(Arrays.asList(new Object[] { "a", 1l }),
				scriptExecutor.execute(script, Collections.singletonList("mylist")));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExecuteValueResult() {
		this.template = new StringRedisTemplate();
		template.setConnectionFactory(connFactory);
		template.afterPropertiesSet();
		RedisScript<String> script = new DefaultRedisScript<String>(new StaticScriptSource(
				"return redis.call('GET',KEYS[1])"), String.class);
		template.opsForValue().set("foo", "bar");
		ScriptExecutor<String> scriptExecutor = new DefaultScriptExecutor<String>(template);
		assertEquals("bar", scriptExecutor.execute(script, Collections.singletonList("foo")));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testExecuteStatusResult() {
		this.template = new RedisTemplate<String, Long>();
		template.setKeySerializer(new StringRedisSerializer());
		template.setValueSerializer(new GenericToStringSerializer<Long>(Long.class));
		template.setConnectionFactory(connFactory);
		template.afterPropertiesSet();
		RedisScript script = new DefaultRedisScript(new StaticScriptSource(
				"return redis.call('SET',KEYS[1], ARGV[1])"), null);
		ScriptExecutor<String> scriptExecutor = new DefaultScriptExecutor<String>(template);
		assertNull(scriptExecutor.execute(script, Collections.singletonList("foo"), 3l));
		assertEquals(Long.valueOf(3), template.opsForValue().get("foo"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExecuteCustomResultSerializer() {
		JacksonJsonRedisSerializer<Person> personSerializer = new JacksonJsonRedisSerializer<Person>(
				Person.class);
		this.template = new RedisTemplate<String, Person>();
		template.setKeySerializer(new StringRedisSerializer());
		template.setValueSerializer(personSerializer);
		template.setConnectionFactory(connFactory);
		template.afterPropertiesSet();
		RedisScript<String> script = new DefaultRedisScript<String>(new StaticScriptSource(
				"redis.call('SET',KEYS[1], ARGV[1])\nreturn 'FOO'"), String.class);
		ScriptExecutor<String> scriptExecutor = new DefaultScriptExecutor<String>(template);
		Person joe = new Person("Joe", "Schmoe", 23);
		String result = scriptExecutor.execute(script, personSerializer,
				new StringRedisSerializer(), Collections.singletonList("bar"), joe);
		assertEquals("FOO", result);
		assertEquals(joe, template.boundValueOps("bar").get());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExecutePipelined() {
		this.template = new StringRedisTemplate();
		template.setConnectionFactory(connFactory);
		template.afterPropertiesSet();
		final RedisScript<String> script = new DefaultRedisScript<String>(new StaticScriptSource(
				"return KEYS[1]"), String.class);
		List<Object> results = template.executePipelined(new SessionCallback<String>() {
			@SuppressWarnings("rawtypes")
			public String execute(RedisOperations operations) throws DataAccessException {
				return (String) operations.execute(script, Collections.singletonList("foo"));
			}

		});
		// Result is deserialized by RedisTemplate as part of executePipelined
		assertEquals(Collections.singletonList("foo"), results);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExecuteTx() {
		this.template = new StringRedisTemplate();
		template.setConnectionFactory(connFactory);
		template.afterPropertiesSet();
		final RedisScript<String> script = new DefaultRedisScript<String>(new StaticScriptSource(
				"return 'bar'..KEYS[1]"), String.class);
		List<Object> results = (List<Object>) template.execute(new SessionCallback<List<Object>>() {
			@SuppressWarnings("rawtypes")
			public List<Object> execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.execute(script, Collections.singletonList("foo"));
				return operations.exec();
			}

		});
		// Result is deserialized by RedisTemplate as part of exec
		assertEquals(Collections.singletonList("barfoo"), results);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExecuteCachedNullKeys() {
		this.template = new StringRedisTemplate();
		template.setConnectionFactory(connFactory);
		template.afterPropertiesSet();
		final RedisScript<String> script = new DefaultRedisScript<String>(new StaticScriptSource(
				"return 'HELLO'"), String.class);
		ScriptExecutor<String> scriptExecutor = new DefaultScriptExecutor<String>(template);
		// Execute script twice, second time should be from cache
		assertEquals("HELLO", scriptExecutor.execute(script, null));
		assertEquals("HELLO", scriptExecutor.execute(script, null));
	}
}
