/*
 * Copyright 2026-present the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisJsonCommands;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.extension.RedisStandalone;

/**
 * Integration test of {@link DefaultJsonOperations}.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
@ParameterizedClass
@MethodSource("testParams")
class DefaultJsonOperationsIntegrationTests<K> {

	private final RedisJsonTemplate<K> template;
	private final ObjectFactory<K> keyFactory;
	private final JsonOperations<K> jsonOps;

	public DefaultJsonOperationsIntegrationTests(RedisJsonTemplate<K> template, ObjectFactory<K> keyFactory) {

		this.template = template;
		this.keyFactory = keyFactory;
		this.jsonOps = template.opsForJson();
	}

	static Collection<Object[]> testParams() {

		List<Object[]> params = new ArrayList<>();
		params.addAll(testParams(LettuceConnectionFactoryExtension.getConnectionFactory(RedisStandalone.class)));
		params.addAll(testParams(JedisConnectionFactoryExtension.getConnectionFactory(RedisStandalone.class)));
		return params;
	}

	static Collection<Object[]> testParams(RedisConnectionFactory connectionFactory) {

		ObjectFactory<String> stringFactory = new StringObjectFactory();

		RedisJsonTemplate<String> stringTemplate = new RedisJsonTemplate<>();
		stringTemplate.setKeySerializer(StringRedisSerializer.UTF_8);
		stringTemplate.setConnectionFactory(connectionFactory);
		stringTemplate.afterPropertiesSet();

		return Arrays.asList(new Object[][] { { stringTemplate, stringFactory } });
	}

	@BeforeEach
	void setUp() {
		template.execute(connection -> {
			connection.serverCommands().flushDb();
			return null;
		});
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.ARRAPPEND")
	void testArrayAppend() {

		K key = keyFactory.instance();

		jsonOps.set(key, DRAGON_REBORN);

		assertThat(jsonOps.array(key).path("$.forsakenDefeated").append(4, 5, 6))
				.isEqualTo(List.of(6L));
		assertThat(jsonOps.key(key).path("$.forsakenDefeated").get()
				.as(new ParameterizedTypeReference<List<List<Long>>>() {}))
				.isEqualTo(List.of(List.of(1L, 2L, 3L, 4L, 5L, 6L)));
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.ARRINDEX")
	void testArrayIndex() {

		K key = keyFactory.instance();

		jsonOps.set(key, DRAGON_REBORN);

		assertThat(jsonOps.array(key).path("$.forsakenDefeated").indexOf(2L))
				.isEqualTo(List.of(1L));
		assertThat(jsonOps.array(key).path("$.forsakenDefeated").indexOf(Integer.MAX_VALUE))
				.isEqualTo(List.of(-1L));
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.ARRINSERT")
	void testArrayInsert() {

		K key = keyFactory.instance();

		jsonOps.set(key, DRAGON_REBORN);

		assertThat(jsonOps.array(key).path("$.forsakenDefeated").index(2).insert(1, 4, 5, 6))
				.isEqualTo(List.of(7L));
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.ARRLEN")
	void testArrayLength() {

		K key = keyFactory.instance();

		jsonOps.set(key, DRAGON_REBORN);

		assertThat(jsonOps.array(key).path("$.forsakenDefeated").length())
				.isEqualTo(List.of(3L));
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.ARRTRIM")
	void testArrayTrim() {

		K key = keyFactory.instance();

		jsonOps.set(key, DRAGON_REBORN);

		assertThat(jsonOps.array(key).path("$.forsakenDefeated").trim(1, 2))
				.isEqualTo(List.of(2L));
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.CLEAR")
	void testClear() {

		K key = keyFactory.instance();

		jsonOps.set(key, DRAGON_REBORN);

		assertThat(jsonOps.key(key).clear()).isEqualTo(1);
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.DEL")
	void testDelete() {

		K key = keyFactory.instance();

		jsonOps.set(key, DRAGON_REBORN);

		assertThat(jsonOps.key(key).delete()).isEqualTo(1);
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.GET")
	void testGet() {

		K key = keyFactory.instance();

		jsonOps.set(key, DRAGON_REBORN);

		assertThat(jsonOps.get(key).as(new ParameterizedTypeReference<List<DragonReborn>>() {}))
				.isEqualTo(List.of(DRAGON_REBORN));
		assertThat(jsonOps.paths(key, "$.name", "$.age").asString())
				.contains("Rand al'Thor")
				.contains("34");
		assertThat(jsonOps.paths(key, List.of("$.name")).asString())
				.contains("Rand al'Thor");
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.MERGE")
	void testMerge() {

		K key = keyFactory.instance();

		jsonOps.set(key, DRAGON_REBORN);

		assertThat(jsonOps.key(key).mergeWith(Map.of("age", 35))).isTrue();
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.MGET")
	void testMultiGet() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		K missing = keyFactory.instance();

		jsonOps.set(key1, DRAGON_REBORN);
		jsonOps.set(key2, DRAGON_REBORN);

		assertThat(jsonOps.values(List.of(key1)).get().as(new ParameterizedTypeReference<List<DragonReborn>>() {}))
				.isEqualTo(List.of(List.of(DRAGON_REBORN)));
		assertThat(jsonOps.values(List.of(key1, key2)).get().as(new ParameterizedTypeReference<List<DragonReborn>>() {}))
				.isEqualTo(List.of(List.of(DRAGON_REBORN), List.of(DRAGON_REBORN)));
		assertThat(jsonOps.values(List.of(key1, missing, key2)).get()
				.as(new ParameterizedTypeReference<List<DragonReborn>>() {}))
				.containsExactly(List.of(DRAGON_REBORN), null, List.of(DRAGON_REBORN));
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.SET")
	void testSet() {

		K key = keyFactory.instance();

		assertThat(jsonOps.key(key).setIfPresent(DRAGON_REBORN)).isNotEqualTo(Boolean.TRUE);
		assertThat(jsonOps.key(key).setIfAbsent(DRAGON_REBORN)).isTrue();
		assertThat(jsonOps.key(key).setIfAbsent(CALLANDOR)).isNotEqualTo(Boolean.TRUE);
		assertThat(jsonOps.key(key).setIfPresent(CALLANDOR)).isTrue();
		assertThat(jsonOps.key(key).conditional(JsonOperations.JsonSetSpec::ifPresent).set(DRAGON_REBORN)).isTrue();
		assertThat(jsonOps.set(key, DRAGON_REBORN)).isTrue();
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.STRAPPEND")
	void testStringAppend() {

		K key = keyFactory.instance();

		jsonOps.set(key, DRAGON_REBORN);

		assertThat(jsonOps.string(key).path("$.name").append("foo"))
				.isEqualTo(List.of(15L));
		assertThat(jsonOps.string(key).path("$.name").get().as(new ParameterizedTypeReference<List<String>>() {}))
				.isEqualTo(List.of("Rand al'Thorfoo"));
		assertThat(jsonOps.string(key).path("$.name").append("\"x\\y"))
				.isEqualTo(List.of(19L));
		assertThat(jsonOps.string(key).path("$.name").get().as(new ParameterizedTypeReference<List<String>>() {}))
				.isEqualTo(List.of("Rand al'Thorfoo\"x\\y"));
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.STRLEN")
	void testStringLength() {

		K key = keyFactory.instance();

		jsonOps.set(key, DRAGON_REBORN);

		assertThat(jsonOps.string(key).path("$.name").length())
				.isEqualTo(List.of(12L));
		assertThat(jsonOps.string(key).path("$.name").set("Lews Therin"))
				.isTrue();
		assertThat(jsonOps.string(key).path("$.name").length())
				.isEqualTo(List.of(11L));
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.TOGGLE")
	void testToggle() {

		K key = keyFactory.instance();

		jsonOps.set(key, DRAGON_REBORN);

		assertThat(jsonOps.bool(key).path("$.madness").toggle())
				.isEqualTo(List.of(true));
		assertThat(jsonOps.bool(key).path("$.madness").set(false))
				.isTrue();
		assertThat(jsonOps.bool(key).path("$.madness").toggle())
				.isEqualTo(List.of(true));
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.TYPE")
	void testType() {

		K key = keyFactory.instance();

		jsonOps.set(key, DRAGON_REBORN);

		assertThat(jsonOps.key(key).path("$.name").getType())
				.isEqualTo(List.of(RedisJsonCommands.JsonType.STRING));
		assertThat(jsonOps.key(key).root().getType())
				.isEqualTo(List.of(RedisJsonCommands.JsonType.OBJECT));
	}

	record Callandor(
			String name,
			double length,
			double width
	) {}

	record DragonReborn(
			String name,
			long age,
			boolean madness,
			List<String> titles,
			List<Long> forsakenDefeated,
			Callandor callandor
	) {}

	private static final Callandor CALLANDOR = new Callandor("Callandor", 10.0, 1.0);

	private static final DragonReborn DRAGON_REBORN = new DragonReborn(
			"Rand al'Thor",
			34,
			false,
			List.of("Dragon Reborn", "Lord of the Morning"),
			List.of(1L, 2L, 3L),
			CALLANDOR
	);

}
