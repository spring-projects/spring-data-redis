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
import static org.assertj.core.api.Assumptions.*;
import static org.junit.jupiter.params.provider.Arguments.*;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.json.JsonType;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.condition.RedisDetector;
import org.springframework.data.redis.test.extension.RedisCluster;
import org.springframework.data.redis.test.extension.RedisStandalone;

/**
 * Integration test of {@link RedisJsonTemplate}.
 *
 * @author Yordan Tsintsov
 * @author Mark Paluch
 * @since 4.2
 */
@ParameterizedClass
@MethodSource("testParams")
class RedisJsonTemplateIntegrationTests<K> {

	static Callandor CALLANDOR = new Callandor("Callandor", 10.0, 1.0);

	static DragonReborn DRAGON_REBORN = new DragonReborn("Rand al'Thor", 34, false,
			List.of("Dragon Reborn", "Lord of the Morning"), List.of(1L, 2L, 3L), CALLANDOR);

	static GenericJackson2JsonRedisSerializer JACKSON2 = GenericJackson2JsonRedisSerializer.builder().defaultTyping(false)
			.build();
	static GenericJacksonJsonRedisSerializer JACKSON3 = GenericJacksonJsonRedisSerializer.builder().build();

	private final RedisConnectionFactory connectionFactory;
	private final RedisJsonTemplate<K> template;
	private final ObjectFactory<K> keyFactory;

	public RedisJsonTemplateIntegrationTests(RedisConnectionFactory connectionFactory, RedisJsonTemplate<K> template,
			ObjectFactory<K> keyFactory) {
		this.connectionFactory = connectionFactory;
		this.template = template;
		this.keyFactory = keyFactory;
	}

	static Collection<Arguments> testParams() {

		List<Arguments> params = new ArrayList<>();
		params.addAll(testParams("Lettuce", LettuceConnectionFactoryExtension::getConnectionFactory));
		params.addAll(testParams("Jedis", JedisConnectionFactoryExtension::getConnectionFactory));
		return params;
	}

	static Collection<Arguments> testParams(String label,
			Function<Class<? extends Annotation>, RedisConnectionFactory> factoryFunction) {

		List<Arguments> params = new ArrayList<>();

		ObjectFactory<String> stringFactory = new StringObjectFactory();

		RedisConnectionFactory standalone = factoryFunction.apply(RedisStandalone.class);
		params.add(argumentSet(label + "/Jackson 2", standalone,
				new RedisJsonTemplate<>(standalone, StringRedisSerializer.UTF_8, JACKSON2), stringFactory));
		params.add(argumentSet(label + "/Jackson 3", standalone,
				new RedisJsonTemplate<>(standalone, StringRedisSerializer.UTF_8, JACKSON3), stringFactory));

		if (RedisDetector.isClusterAvailable()) {
			RedisConnectionFactory cluster = factoryFunction.apply(RedisCluster.class);
			params.add(argumentSet(label + " Cluster/Jackson 2", cluster,
					new RedisJsonTemplate<>(cluster, StringRedisSerializer.UTF_8, JACKSON2), stringFactory));
			params.add(argumentSet(label + "Cluster/Jackson 3", cluster,
					new RedisJsonTemplate<>(cluster, StringRedisSerializer.UTF_8, JACKSON3), stringFactory));
		}

		return params;
	}

	@BeforeEach
	void setUp() {
		try (RedisConnection connection = connectionFactory.getConnection()) {
			connection.serverCommands().flushDb();
		}
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.ARRAPPEND")
	void arrayAppend() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		assertThat(template.array(key).path("$.forsakenDefeated").append(4, 5, 6)).containsOnly(6L);
		assertThat(
				template.value(key).path("$.forsakenDefeated").get().as(new ParameterizedTypeReference<List<List<Long>>>() {}))
				.containsOnly(List.of(1L, 2L, 3L, 4L, 5L, 6L));

		assertThat(template.array(key).path("$.absent").append(4, 5, 6)).isEmpty();
		assertThatExceptionOfType(RuntimeException.class)
				.isThrownBy(() -> template.array(keyFactory.instance()).path("$.absent").append(4, 5, 6));

		K arrayTopLevel = keyFactory.instance();

		template.set(arrayTopLevel, List.of(Map.of("name", "Rand al'Thor")));
		assertThat(template.array(key).append(Map.of("name", "Walter"))).containsOnlyNulls();
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.ARRINDEX")
	void arrayIndex() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		assertThat(template.array(key).path("$.forsakenDefeated").indexOf(2L)).containsOnly(1L);
		assertThat(template.array(key).path("$.forsakenDefeated").indexOf(Integer.MAX_VALUE)).containsOnly(-1L);

		assertThat(template.array(key).path("$.absent").indexOf(4)).isEmpty();
		assertThatExceptionOfType(RuntimeException.class)
				.isThrownBy(() -> template.array(keyFactory.instance()).path("$.absent").indexOf(4));

		List<Long> indexes = template.array(key).path("$.*").indexOf(2L);
		assertThat(indexes).containsExactly(null, null, null, -1L, 1L, null);
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.ARRINSERT")
	void arrayInsert() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		assertThat(template.array(key).path("$.forsakenDefeated").index(2).insert(1, 4, 5, 6)).containsOnly(7L);

		assertThat(template.array(key).path("$.absent").index(2).insert(1, 2, 3)).isEmpty();
		assertThatExceptionOfType(RuntimeException.class)
				.isThrownBy(() -> template.array(keyFactory.instance()).path("$.absent").index(2).insert(1, 2, 3));
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.ARRLEN")
	void arrayLength() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		assertThat(template.array(key).path("$.forsakenDefeated").length()).containsOnly(3L);
		assertThat(template.array(key).path("$.absent").length()).isEmpty();
		assertThatExceptionOfType(RuntimeException.class)
				.isThrownBy(() -> template.array(keyFactory.instance()).path("$.absent").length());
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.ARRTRIM")
	void arrayTrim() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		assertThat(template.array(key).path("$.forsakenDefeated").trim(1, 2)).containsOnly(2L);

		assertThat(template.array(key).path("$.absent").trim(1, 2)).isEmpty();
		assertThatExceptionOfType(RuntimeException.class)
				.isThrownBy(() -> template.array(keyFactory.instance()).path("$.absent").trim(1, 2));
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.CLEAR")
	void clear() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		assertThat(template.value(key).clear()).isOne();
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.DEL")
	void deleteValue() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		assertThat(template.value(key).delete()).isOne();
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.GET")
	void get() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		JsonOperations.JsonResult result = template.get(key);

		// native: array wrapping per item
		assertThat(result.as(new ParameterizedTypeReference<List<DragonReborn>>() {})).containsOnly(DRAGON_REBORN);

		// single-element array unwrapping
		assertThat(result.as(DragonReborn.class)).isEqualTo(DRAGON_REBORN);
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.GET")
	@Disabled("TODO: Not sure whether unwrapping behind the scenes is a good idea")
	void getArray() {

		K key = keyFactory.instance();

		template.set(key, List.of(Map.of("name", "Rand al'Thor")));

		JsonOperations.JsonResult result = template.get(key);

		// single-element array unwrapping
		assertThat(result.as(String.class)).isEqualTo("{\"name\":\"Rand al'Thor\"}");
		assertThat(result.asString()).isEqualTo("{\"name\":\"Rand al'Thor\"}");
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.GET")
	void getAndMap() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		assertThat((String) template.get(key).map(String::new)).contains("[{\"").contains("\"madness\":false");
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.GET")
	void getSingleElement() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		assertThat(template.get(key).as(DragonReborn.class)).isEqualTo(DRAGON_REBORN);
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.GET")
	void pathsReturnWrappedArrays() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		JsonOperations.JsonResult paths = template.paths(key, "$.name", "$.age");
		assertThat(paths.asString()).contains("Rand al'Thor").contains("34");
		assertThat(paths.as(Map.class)).isEqualTo(Map.of("$.name", List.of("Rand al'Thor"), "$.age", List.of(34)));
		assertThat(template.paths(key, List.of("$.name")).asString()).contains("Rand al'Thor");
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.GET")
	void pathsWithPropertyReturnProperMap() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		JsonOperations.JsonResult paths = template.paths(key, "name");
		assertThat(paths.asString()).isEqualTo("{\"name\":\"Rand al'Thor\"}");
		assertThat(paths.as(Map.class)).isEqualTo(Map.of("name", "Rand al'Thor"));

		paths = template.paths(key, "name", "age");
		assertThat(paths.asString()).contains("\"name\":\"Rand al'Thor\"").contains("\"age\":34");
		assertThat(paths.as(Map.class)).isEqualTo(Map.of("name", "Rand al'Thor", "age", 34));
	}

	@Test // GH-3390
	void pathsDoesNotSupportMixingPropertyAndJSONPathExpressions() {

		K key = keyFactory.instance();
		assertThatIllegalArgumentException().isThrownBy(() -> template.paths(key, "name", "$.name"))
				.withMessage("Mixing bare property names and JSONPath expressions is not supported");
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.GET")
	void pathsWithPropertiesReturnProperMap() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		JsonOperations.JsonResult paths = template.paths(key, "name", "age", "madness");
		assertThat(paths.asString()).contains("Rand al'Thor").contains("34");

		DragonReborn DRAGON_REBORN = new DragonReborn("Rand al'Thor", 34, false, null, null, null);
		assertThat(paths.as(DragonReborn.class)).isEqualTo(DRAGON_REBORN);
		assertThat(paths.as(Map.class)).isEqualTo(Map.of("name", "Rand al'Thor", "age", 34, "madness", false));
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.GET")
	void getAbsentKey() {

		K key = keyFactory.instance();

		assertThat(template.get(key).isNull()).isTrue();
		JsonOperations.JsonResult jsonResult = template.value(key).get();
		assertThat(jsonResult.isNull()).isTrue();
		assertThat(jsonResult.toString()).isEqualTo("null");
		assertThat(jsonResult.asBytes()).isEqualTo("null".getBytes());
		assertThat((Object) jsonResult.map(it -> new Object())).isNull();

		assertThat((String) template.get(keyFactory.instance()).map(String::new)).isNull();
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.GET")
	void getNullProjection() {

		K key = keyFactory.instance();

		template.set(key, Collections.singletonMap("foo", null));

		JsonOperations.JsonResult jsonResult = template.value(key).path("$.foo").get();
		assertThat(jsonResult.isNull()).isFalse();
		assertThat(jsonResult.toString()).isEqualTo("[null]");
		assertThat(jsonResult.asBytes()).isEqualTo("[null]".getBytes());
		assertThat((Object) jsonResult.map(it -> new Object())).isNotNull();
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.MERGE")
	void merge() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		assertThat(template.value(key).mergeWith(Map.of("age", 35))).isTrue();
		assertThat(template.value(key).path("$.age").get().as(Integer.class)).isEqualTo(35);
		assertThat(template.value(key).path("$.age").get().asString()).isEqualTo("35");
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.MGET")
	void multiGet() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		K missing = keyFactory.instance();

		template.set(key1, DRAGON_REBORN);
		template.set(key2, DRAGON_REBORN);

		assertThat(template.values(key1).get().as(DragonReborn.class)).containsOnly(DRAGON_REBORN);
		assertThat(template.values(key1, key2).get().as(DragonReborn.class)).containsOnly(DRAGON_REBORN, DRAGON_REBORN);
		assertThat(template.values(key1, missing, key2).get().as(DragonReborn.class)).containsExactly(DRAGON_REBORN, null,
				DRAGON_REBORN);
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.SET")
	void testSet() {

		K key = keyFactory.instance();

		assertThat(template.value(key).setIfPresent(DRAGON_REBORN)).isFalse();
		assertThat(template.value(key).setIfAbsent(DRAGON_REBORN)).isTrue();
		assertThat(template.value(key).setIfAbsent(CALLANDOR)).isFalse();
		assertThat(template.value(key).setIfPresent(CALLANDOR)).isTrue();
		assertThat(template.value(key).conditional(JsonOperations.JsonSetSpec::ifPresent).set(DRAGON_REBORN)).isTrue();
		assertThat(template.value(key).conditional(JsonOperations.JsonSetSpec::ifPresent).set(DRAGON_REBORN)).isTrue();
		assertThat(template.set(key, DRAGON_REBORN)).isTrue();
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.SET")
	void setConditional() {

		K key = keyFactory.instance();

		assertThat(template.value(key).setIfPresent(DRAGON_REBORN)).isFalse();
		assertThat(template.value(key).setIfAbsent(DRAGON_REBORN)).isTrue();

		assertThat(template.value(key).setIfAbsent(CALLANDOR)).isFalse();
		assertThat(template.value(key).setIfPresent(CALLANDOR)).isTrue();
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.SET")
	void setConditionalPath() {

		K key = keyFactory.instance();

		JsonOperations.JsonAtKeySpec at = template.value(key);

		assertThat(at.path("$.name").setIfPresent("Mo")).isFalse();
		assertThat(at.path("$").setIfAbsent(Map.of("hello", "world"))).isTrue();
		assertThat(at.path("$").setIfAbsent(Map.of("hello", "world"))).isFalse();

		assertThat(at.path("$.name").setIfAbsent("Mo")).isTrue();
		assertThat(at.path("$.name").setIfPresent("Mo")).isTrue();
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.SET")
	void setConditionalPathOnString() {

		K key = keyFactory.instance();

		JsonOperations.JsonAtKeySpec at = template.value(key);

		assertThat(at.set("Mo")).isTrue();

		assertThat(at.path("$.name").setIfPresent("Mo")).isFalse();

		// cannot set a path if the value is a String
		assertThat(at.path("$.name").setIfAbsent("Mo")).isFalse();
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.STRAPPEND")
	void stringAppend() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		assertThat(template.string(key).path("$.name").append("foo")).containsOnly(15L);
		assertThat(template.string(key).path("$.name").get().asString()).isEqualTo("Rand al'Thorfoo");
		assertThat(template.string(key).path("$.name").append("\"x\\y")).containsOnly(19L);
		assertThat(template.string(key).path("$.name").get().asString()).isEqualTo("Rand al'Thorfoo\"x\\y");
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.STRLEN")
	void stringLength() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		assertThat(template.string(key).path("$.name").length()).containsOnly(12L);
		assertThat(template.string(key).path("$.name").set("Lews Therin")).isTrue();
		assertThat(template.string(key).path("$.name").length()).containsOnly(11L);
		assertThat(template.string(key).path("$.foo").length()).isEmpty();

		assertThatExceptionOfType(RuntimeException.class)
				.isThrownBy(() -> template.string(keyFactory.instance()).path("$.name").length());
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.TOGGLE")
	void toggle() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		assertThat(template.bool(key).path("$.madness").toggle()).containsOnly(true);
		assertThat(template.bool(key).path("$.madness").set(false)).isTrue();
		assertThat(template.bool(key).path("$.madness").toggle()).containsOnly(true);

		assertThat(template.bool(key).path("$.absent").toggle()).isEmpty();

		// TODO: InvalidDataAccessApiUsageException Jedis vs. RedisSystemException Lettuce
		assertThatExceptionOfType(RuntimeException.class)
				.isThrownBy(() -> template.bool(keyFactory.instance()).path("$.absent").toggle());
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.TYPE")
	void type() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		assertThat(template.value(key).path("$.name").getType()).containsOnly(JsonType.STRING);
		assertThat(template.value(key).root().getType()).containsOnly(JsonType.OBJECT);
		assertThat(template.value(key).path("$.foo").getType()).isEmpty();

		StringRedisTemplate redisTemplate = new StringRedisTemplate();
		redisTemplate.setConnectionFactory(connectionFactory);
		redisTemplate.afterPropertiesSet();

		assertThat(redisTemplate.type(key.toString())).isEqualTo(DataType.JSON);

		// TODO: Driver failures
		// assertThatExceptionOfType(RuntimException.class).isThrownBy(() -> template.value(keyFactory.instance())
		// .path("$.name").getType());
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.SET")
	void scan() {

		try (RedisConnection connection = connectionFactory.getConnection()) {
			assumeThat(connection).isNotInstanceOf(JedisClusterConnection.class);
		}

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);

		StringRedisTemplate redisTemplate = new StringRedisTemplate();
		redisTemplate.setConnectionFactory(connectionFactory);
		redisTemplate.afterPropertiesSet();

		try (Cursor<String> scan = redisTemplate.scan(ScanOptions.scanOptions().type(DataType.JSON).build())) {
			assertThat(scan.hasNext()).isTrue();
			assertThat(scan.next()).isEqualTo(key.toString());
		}
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.SET")
	void keyDeleteUnlink() {

		K key = keyFactory.instance();

		template.set(key, DRAGON_REBORN);
		assertThat(template.delete(key)).isTrue();

		assertThat(template.get(key).isNull()).isTrue();

		template.set(key, DRAGON_REBORN);

		assertThat(template.key(key).unlink()).isTrue();
		assertThat(template.get(key).isNull()).isTrue();
	}

	@Test // GH-3390
	@EnabledOnCommand("JSON.SET")
	void keysDeleteUnlink() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();

		template.set(key1, DRAGON_REBORN);
		template.set(key2, DRAGON_REBORN);

		assertThat(template.keys(key1, key2).unlink()).isEqualTo(2);
		assertThat(template.keys(key1, key2).unlink()).isEqualTo(0);

		template.set(key1, DRAGON_REBORN);
		template.set(key2, DRAGON_REBORN);

		assertThat(template.keys(key1, key2).delete()).isEqualTo(2);
		assertThat(template.get(key1).isNull()).isTrue();
		assertThat(template.get(key2).isNull()).isTrue();
	}

	record Callandor(String name, double length, double widt) {
	}

	record DragonReborn(String name, long age, boolean madness, List<String> titles, List<Long> forsakenDefeated,
			Callandor callandor) {
	}

}
