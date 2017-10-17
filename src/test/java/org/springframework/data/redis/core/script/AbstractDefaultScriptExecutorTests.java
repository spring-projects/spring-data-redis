/*
 * Copyright 2013-2017 the original author or authors.
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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.test.util.MinimumRedisVersionRule;
import org.springframework.scripting.support.StaticScriptSource;
import org.springframework.test.annotation.IfProfileValue;

/**
 * Integration test of {@link DefaultScriptExecutor}
 *
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@IfProfileValue(name = "redisVersion", value = "2.6+")
public abstract class AbstractDefaultScriptExecutorTests {

	public static @ClassRule MinimumRedisVersionRule minRedisVersion = new MinimumRedisVersionRule();

	@SuppressWarnings("rawtypes")//
	private RedisTemplate template;

	protected abstract RedisConnectionFactory getConnectionFactory();

	@SuppressWarnings("unchecked")
	public void tearDown() {

		if (template == null) {
			return;
		}

		template.execute((RedisCallback<Object>) connection -> {
			connection.flushDb();
			connection.scriptFlush();
			return null;
		});
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExecuteLongResult() {
		this.template = new StringRedisTemplate();
		template.setConnectionFactory(getConnectionFactory());
		template.afterPropertiesSet();
		DefaultRedisScript<Long> script = new DefaultRedisScript<>();
		script.setLocation(new ClassPathResource("org/springframework/data/redis/core/script/increment.lua"));
		script.setResultType(Long.class);
		ScriptExecutor<String> scriptExecutor = new DefaultScriptExecutor<String>(template);
		Long result = scriptExecutor.execute(script, Collections.singletonList("mykey"));
		assertNull(result);
		template.boundValueOps("mykey").set("2");
		assertEquals(Long.valueOf(3), scriptExecutor.execute(script, Collections.singletonList("mykey")));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExecuteBooleanResult() {
		this.template = new RedisTemplate<String, Long>();
		template.setKeySerializer(StringRedisSerializer.UTF_8);
		template.setValueSerializer(new GenericToStringSerializer<>(Long.class));
		template.setConnectionFactory(getConnectionFactory());
		template.afterPropertiesSet();
		DefaultRedisScript<Boolean> script = new DefaultRedisScript<>();
		script.setLocation(new ClassPathResource("org/springframework/data/redis/core/script/cas.lua"));
		script.setResultType(Boolean.class);
		ScriptExecutor<String> scriptExecutor = new DefaultScriptExecutor<String>(template);
		template.boundValueOps("counter").set(0l);
		Boolean valueSet = scriptExecutor.execute(script, Collections.singletonList("counter"), 0, 3);
		assertTrue(valueSet);
		assertFalse(scriptExecutor.execute(script, Collections.singletonList("counter"), 0, 3));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testExecuteListResultCustomArgsSerializer() {
		this.template = new StringRedisTemplate();
		template.setConnectionFactory(getConnectionFactory());
		template.afterPropertiesSet();
		template.boundListOps("mylist").leftPushAll("a", "b", "c", "d");
		DefaultRedisScript<List> script = new DefaultRedisScript<>();
		script.setLocation(new ClassPathResource("org/springframework/data/redis/core/script/bulkpop.lua"));
		script.setResultType(List.class);
		ScriptExecutor<String> scriptExecutor = new DefaultScriptExecutor<String>(template);
		List<String> result = scriptExecutor.execute(script, new GenericToStringSerializer<>(Long.class),
				template.getValueSerializer(), Collections.singletonList("mylist"), 1l);
		assertEquals(Collections.singletonList("a"), result);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testExecuteMixedListResult() {
		this.template = new StringRedisTemplate();
		template.setConnectionFactory(getConnectionFactory());
		template.afterPropertiesSet();
		DefaultRedisScript<List> script = new DefaultRedisScript<>();
		script.setLocation(new ClassPathResource("org/springframework/data/redis/core/script/popandlength.lua"));
		script.setResultType(List.class);
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
		template.setConnectionFactory(getConnectionFactory());
		template.afterPropertiesSet();
		DefaultRedisScript<String> script = new DefaultRedisScript<>();
		script.setScriptText("return redis.call('GET',KEYS[1])");
		script.setResultType(String.class);
		template.opsForValue().set("foo", "bar");
		ScriptExecutor<String> scriptExecutor = new DefaultScriptExecutor<String>(template);
		assertEquals("bar", scriptExecutor.execute(script, Collections.singletonList("foo")));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testExecuteStatusResult() {
		this.template = new RedisTemplate<String, Long>();
		template.setKeySerializer(StringRedisSerializer.UTF_8);
		template.setValueSerializer(new GenericToStringSerializer<>(Long.class));
		template.setConnectionFactory(getConnectionFactory());
		template.afterPropertiesSet();
		DefaultRedisScript script = new DefaultRedisScript();
		script.setScriptText("return redis.call('SET',KEYS[1], ARGV[1])");
		ScriptExecutor<String> scriptExecutor = new DefaultScriptExecutor<String>(template);
		assertNull(scriptExecutor.execute(script, Collections.singletonList("foo"), 3l));
		assertEquals(Long.valueOf(3), template.opsForValue().get("foo"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExecuteCustomResultSerializer() {
		Jackson2JsonRedisSerializer<Person> personSerializer = new Jackson2JsonRedisSerializer<>(Person.class);
		this.template = new RedisTemplate<String, Person>();
		template.setKeySerializer(StringRedisSerializer.UTF_8);
		template.setValueSerializer(personSerializer);
		template.setConnectionFactory(getConnectionFactory());
		template.afterPropertiesSet();
		DefaultRedisScript<String> script = new DefaultRedisScript<>();
		script.setScriptSource(new StaticScriptSource("redis.call('SET',KEYS[1], ARGV[1])\nreturn 'FOO'"));
		script.setResultType(String.class);
		ScriptExecutor<String> scriptExecutor = new DefaultScriptExecutor<String>(template);
		Person joe = new Person("Joe", "Schmoe", 23);
		String result = scriptExecutor.execute(script, personSerializer, StringRedisSerializer.UTF_8,
				Collections.singletonList("bar"), joe);
		assertEquals("FOO", result);
		assertEquals(joe, template.boundValueOps("bar").get());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExecutePipelined() {
		this.template = new StringRedisTemplate();
		template.setConnectionFactory(getConnectionFactory());
		template.afterPropertiesSet();
		final DefaultRedisScript<String> script = new DefaultRedisScript<>();
		script.setScriptText("return KEYS[1]");
		script.setResultType(String.class);
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
		template.setConnectionFactory(getConnectionFactory());
		template.afterPropertiesSet();
		final DefaultRedisScript<String> script = new DefaultRedisScript<>();
		script.setScriptText("return 'bar'..KEYS[1]");
		script.setResultType(String.class);
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
		template.setConnectionFactory(getConnectionFactory());
		template.afterPropertiesSet();
		DefaultRedisScript<String> script = new DefaultRedisScript<>();
		script.setScriptText("return 'HELLO'");
		script.setResultType(String.class);
		ScriptExecutor<String> scriptExecutor = new DefaultScriptExecutor<String>(template);
		// Execute script twice, second time should be from cache
		assertEquals("HELLO", scriptExecutor.execute(script, null));
		assertEquals("HELLO", scriptExecutor.execute(script, null));
	}

	@Test // DATAREDIS-356
	public void shouldTransparentlyReEvaluateScriptIfNotPresent() throws Exception {

		this.template = new StringRedisTemplate();
		template.setConnectionFactory(getConnectionFactory());
		template.afterPropertiesSet();

		DefaultRedisScript<String> script = new DefaultRedisScript<>();
		script.setScriptText("return 'BUBU" + System.currentTimeMillis() + "'");
		script.setResultType(String.class);

		ScriptExecutor<String> scriptExecutor = new DefaultScriptExecutor<String>(template);
		assertEquals("BUBU", scriptExecutor.execute(script, null).substring(0, 4));
	}
}
