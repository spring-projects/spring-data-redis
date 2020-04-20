/*
 * Copyright 2017-2020 the original author or authors.
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
package org.springframework.data.redis.core.script;

import static org.assertj.core.api.Assertions.*;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientConfiguration;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisElementReader;
import org.springframework.data.redis.serializer.RedisElementWriter;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializationContext.RedisSerializationContextBuilder;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.scripting.support.StaticScriptSource;

/**
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class DefaultReactiveScriptExecutorTests {

	private static LettuceConnectionFactory connectionFactory;
	private static StringRedisTemplate stringTemplate;
	private static ReactiveScriptExecutor<String> stringScriptExecutor;

	@BeforeClass
	public static void setUp() {

		connectionFactory = new LettuceConnectionFactory(SettingsUtils.standaloneConfiguration(),
				LettuceTestClientConfiguration.create());
		connectionFactory.afterPropertiesSet();

		stringTemplate = new StringRedisTemplate(connectionFactory);
		stringScriptExecutor = new DefaultReactiveScriptExecutor<>(connectionFactory, RedisSerializationContext.string());
	}

	@AfterClass
	public static void cleanUp() {

		if (connectionFactory != null) {
			connectionFactory.destroy();
		}
	}

	@After
	public void tearDown() {

		RedisConnection connection = connectionFactory.getConnection();
		try {
			connection.scriptingCommands().scriptFlush();
			connection.flushDb();
		} finally {
			connection.close();
		}
	}

	protected RedisConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	@Test // DATAREDIS-711
	public void shouldReturnLong() {

		DefaultRedisScript<Long> script = new DefaultRedisScript<>();
		script.setLocation(new ClassPathResource("org/springframework/data/redis/core/script/increment.lua"));
		script.setResultType(Long.class);

		stringScriptExecutor.execute(script, Collections.singletonList("mykey")).as(StepVerifier::create).verifyComplete();

		stringTemplate.opsForValue().set("mykey", "2");

		stringScriptExecutor.execute(script, Collections.singletonList("mykey")).as(StepVerifier::create).expectNext(3L)
				.verifyComplete();
	}

	@Test // DATAREDIS-711
	public void shouldReturnBoolean() {

		RedisSerializationContextBuilder<String, Long> builder = RedisSerializationContext
				.newSerializationContext(StringRedisSerializer.UTF_8);
		builder.value(new GenericToStringSerializer<>(Long.class));

		DefaultRedisScript<Boolean> script = new DefaultRedisScript<>();
		script.setLocation(new ClassPathResource("org/springframework/data/redis/core/script/cas.lua"));
		script.setResultType(Boolean.class);

		ReactiveScriptExecutor<String> scriptExecutor = new DefaultReactiveScriptExecutor<>(connectionFactory,
				builder.build());

		stringTemplate.opsForValue().set("counter", "0");

		scriptExecutor.execute(script, Collections.singletonList("counter"), Arrays.asList(0, 3)).as(StepVerifier::create)
				.expectNext(true).verifyComplete();

		scriptExecutor.execute(script, Collections.singletonList("counter"), Arrays.asList(0, 3)).as(StepVerifier::create)
				.expectNext(false).verifyComplete();
	}

	@Test // DATAREDIS-711
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void shouldApplyCustomArgsSerializer() {

		DefaultRedisScript<List> script = new DefaultRedisScript<>();
		script.setLocation(new ClassPathResource("org/springframework/data/redis/core/script/bulkpop.lua"));
		script.setResultType(List.class);

		stringTemplate.boundListOps("mylist").leftPushAll("a", "b", "c", "d");

		Flux<List<String>> mylist = stringScriptExecutor.execute(script, Collections.singletonList("mylist"),
				Collections.singletonList(1L), RedisElementWriter.from(new GenericToStringSerializer<>(Long.class)),
				(RedisElementReader) RedisElementReader.from(StringRedisSerializer.UTF_8));

		mylist.as(StepVerifier::create).expectNext(Collections.singletonList("a")).verifyComplete();
	}

	@Test // DATAREDIS-711
	public void testExecuteMixedListResult() {

		DefaultRedisScript<List> script = new DefaultRedisScript<>();
		script.setLocation(new ClassPathResource("org/springframework/data/redis/core/script/popandlength.lua"));
		script.setResultType(List.class);

		stringScriptExecutor.execute(script, Collections.singletonList("mylist")).as(StepVerifier::create)
				.expectNext(Arrays.asList(null, 0L)).verifyComplete();

		stringTemplate.boundListOps("mylist").leftPushAll("a", "b");

		stringScriptExecutor.execute(script, Collections.singletonList("mylist")).as(StepVerifier::create)
				.expectNext(Arrays.asList("a", 1L)).verifyComplete();
	}

	@Test // DATAREDIS-711
	public void shouldReturnValueResult() {

		DefaultRedisScript<String> script = new DefaultRedisScript<>();
		script.setScriptText("return redis.call('GET',KEYS[1])");
		script.setResultType(String.class);

		stringTemplate.opsForValue().set("foo", "bar");

		Flux<String> foo = stringScriptExecutor.execute(script, Collections.singletonList("foo"));

		foo.as(StepVerifier::create).expectNext("bar").expectNext();
	}

	@Test // DATAREDIS-711
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void shouldReturnStatusValue() {

		DefaultRedisScript script = new DefaultRedisScript();
		script.setScriptText("return redis.call('SET',KEYS[1], ARGV[1])");

		RedisSerializationContextBuilder<String, Long> builder = RedisSerializationContext
				.newSerializationContext(StringRedisSerializer.UTF_8);
		builder.value(new GenericToStringSerializer<>(Long.class));

		ReactiveScriptExecutor<String> scriptExecutor = new DefaultReactiveScriptExecutor<>(connectionFactory,
				builder.build());

		StepVerifier.create(scriptExecutor.execute(script, Collections.singletonList("foo"), Collections.singletonList(3L)))
				.expectNext("OK").verifyComplete();

		assertThat(stringTemplate.opsForValue().get("foo")).isEqualTo("3");
	}

	@Test // DATAREDIS-711
	public void shouldApplyCustomResultSerializer() {

		Jackson2JsonRedisSerializer<Person> personSerializer = new Jackson2JsonRedisSerializer<>(Person.class);

		RedisTemplate<String, Person> template = new RedisTemplate<>();
		template.setKeySerializer(StringRedisSerializer.UTF_8);
		template.setValueSerializer(personSerializer);
		template.setConnectionFactory(getConnectionFactory());
		template.afterPropertiesSet();

		DefaultRedisScript<String> script = new DefaultRedisScript<>();
		script.setScriptSource(new StaticScriptSource("redis.call('SET',KEYS[1], ARGV[1])\nreturn 'FOO'"));
		script.setResultType(String.class);

		Person joe = new Person("Joe", "Schmoe", 23);
		Flux<String> result = stringScriptExecutor.execute(script, Collections.singletonList("bar"),
				Collections.singletonList(joe), RedisElementWriter.from(personSerializer),
				RedisElementReader.from(StringRedisSerializer.UTF_8));

		result.as(StepVerifier::create).expectNext("FOO").verifyComplete();

		assertThat(template.opsForValue().get("bar")).isEqualTo(joe);
	}

	@Test // DATAREDIS-711
	public void executeAddsScriptToScriptCache() {

		DefaultRedisScript<String> script = new DefaultRedisScript<>();
		script.setScriptText("return 'HELLO'");
		script.setResultType(String.class);

		// Execute script twice, second time should be from cache

		assertThat(stringTemplate.execute(
				(RedisCallback<List<Boolean>>) connection -> connection.scriptingCommands().scriptExists(script.getSha1())))
						.containsExactly(false);

		stringScriptExecutor.execute(script, Collections.emptyList()).as(StepVerifier::create).expectNext("HELLO")
				.verifyComplete();

		assertThat(stringTemplate.execute(
				(RedisCallback<List<Boolean>>) connection -> connection.scriptingCommands().scriptExists(script.getSha1())))
						.containsExactly(true);

		stringScriptExecutor.execute(script, Collections.emptyList()).as(StepVerifier::create).expectNext("HELLO")
				.verifyComplete();
	}
}
