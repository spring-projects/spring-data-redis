/*
 * Copyright 2017-2024 the original author or authors.
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

import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;

import org.springframework.data.redis.ByteBufferObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveOperationsTestParams.Fixture;
import org.springframework.data.redis.test.condition.EnabledIfLongRunningTest;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * Integration tests for {@link DefaultReactiveListOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author John Blum
 */
@MethodSource("testParams")
@SuppressWarnings("unchecked")
public class DefaultReactiveListOperationsIntegrationTests<K, V> {

	private final ReactiveRedisTemplate<K, V> redisTemplate;
	private final ReactiveListOperations<K, V> listOperations;

	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<V> valueFactory;

	public static Collection<Fixture<?, ?>> testParams() {
		return ReactiveOperationsTestParams.testParams();
	}

	public DefaultReactiveListOperationsIntegrationTests(Fixture<K, V> fixture) {

		this.redisTemplate = fixture.getTemplate();
		this.listOperations = redisTemplate.opsForList();
		this.keyFactory = fixture.getKeyFactory();
		this.valueFactory = fixture.getValueFactory();
	}

	@BeforeEach
	void before() {

		RedisConnectionFactory connectionFactory = (RedisConnectionFactory) redisTemplate.getConnectionFactory();
		RedisConnection connection = connectionFactory.getConnection();
		connection.flushAll();
		connection.close();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void trim() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		listOperations.rightPushAll(key, value1, value2) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();

		listOperations.trim(key, 0, 0) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		listOperations.size(key) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void size() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();

		listOperations.size(key) //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();

		listOperations.rightPush(key, value1) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

		listOperations.size(key) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void leftPush() {

		assumeThat(this.valueFactory).isNotInstanceOf(ByteBufferObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		listOperations.leftPush(key, value1) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

		listOperations.leftPush(key, value2) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();

		listOperations.range(key, 0, -1) //
				.as(StepVerifier::create) //
				.expectNext(value2) //
				.expectNext(value1) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void leftPushAll() {

		assumeThat(this.valueFactory).isNotInstanceOf(ByteBufferObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		listOperations.leftPushAll(key, value1, value2) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();

		listOperations.range(key, 0, -1) //
				.as(StepVerifier::create) //
				.expectNext(value2) //
				.expectNext(value1) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void leftPushIfPresent() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		listOperations.leftPushIfPresent(key, value1) //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();

		listOperations.leftPush(key, value1) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

		listOperations.leftPushIfPresent(key, value2) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void leftPushWithPivot() {

		assumeThat(this.valueFactory).isNotInstanceOf(ByteBufferObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		listOperations.leftPushAll(key, value1, value2) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();

		listOperations.leftPush(key, value1, value3) //
				.as(StepVerifier::create) //
				.expectNext(3L) //
				.verifyComplete();

		listOperations.range(key, 0, -1) //
				.as(StepVerifier::create) //
				.expectNext(value2) //
				.expectNext(value3) //
				.expectNext(value1) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void rightPush() {

		assumeThat(this.valueFactory).isNotInstanceOf(ByteBufferObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		listOperations.rightPush(key, value1) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
		listOperations.rightPush(key, value2) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();

		listOperations.range(key, 0, -1) //
				.as(StepVerifier::create) //
				.expectNext(value1) //
				.expectNext(value2) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void rightPushAll() {

		assumeThat(this.valueFactory).isNotInstanceOf(ByteBufferObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		listOperations.rightPushAll(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();

		listOperations.range(key, 0, -1) //
				.as(StepVerifier::create) //
				.expectNext(value1) //
				.expectNext(value2) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void rightPushIfPresent() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		listOperations.rightPushIfPresent(key, value1).as(StepVerifier::create).expectNext(0L).verifyComplete();
		listOperations.rightPush(key, value1).as(StepVerifier::create).expectNext(1L).verifyComplete();
		listOperations.rightPushIfPresent(key, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void rightPushWithPivot() {

		assumeThat(this.valueFactory).isNotInstanceOf(ByteBufferObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		listOperations.rightPushAll(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();

		listOperations.rightPush(key, value1, value3) //
				.as(StepVerifier::create) //
				.expectNext(3L) //
				.verifyComplete();

		listOperations.range(key, 0, -1) //
				.as(StepVerifier::create) //
				.expectNext(value1) //
				.expectNext(value3) //
				.expectNext(value2) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // GH-2039
	@EnabledOnCommand("LMOVE")
	void move() {

		K source = keyFactory.instance();
		K target = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		V v4 = valueFactory.instance();

		listOperations.rightPushAll(source, v1, v2, v3, v4).as(StepVerifier::create).expectNext(4L).verifyComplete();

		listOperations.move(ListOperations.MoveFrom.fromHead(source), ListOperations.MoveTo.toTail(target))
				.as(StepVerifier::create).expectNext(v1).verifyComplete();
		listOperations.move(ListOperations.MoveFrom.fromTail(source), ListOperations.MoveTo.toHead(target))
				.as(StepVerifier::create).expectNext(v4).verifyComplete();

		listOperations.range(source, 0, -1) //
				.as(StepVerifier::create) //
				.expectNext(v2) //
				.expectNext(v3) //
				.verifyComplete();

		listOperations.range(target, 0, -1) //
				.as(StepVerifier::create) //
				.expectNext(v4) //
				.expectNext(v1) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // GH-2039
	@EnabledOnCommand("BLMOVE")
	void moveWithTimeout() {

		K source = keyFactory.instance();
		K target = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v4 = valueFactory.instance();

		listOperations.rightPushAll(source, v1, v4).as(StepVerifier::create).expectNext(2L).verifyComplete();

		listOperations
				.move(ListOperations.MoveFrom.fromHead(source), ListOperations.MoveTo.toTail(target), Duration.ofMillis(10))
				.as(StepVerifier::create).expectNext(v1).verifyComplete();
		listOperations
				.move(ListOperations.MoveFrom.fromTail(source), ListOperations.MoveTo.toHead(target), Duration.ofMillis(10))
				.as(StepVerifier::create).expectNext(v4).verifyComplete();
		listOperations
				.move(ListOperations.MoveFrom.fromTail(source), ListOperations.MoveTo.toHead(target), Duration.ofMillis(10))
				.as(StepVerifier::create).verifyComplete();

		listOperations.range(source, 0, -1) //
				.as(StepVerifier::create) //
				.verifyComplete();

		listOperations.range(target, 0, -1) //
				.as(StepVerifier::create) //
				.expectNext(v4) //
				.expectNext(v1) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void set() {

		assumeThat(this.valueFactory).isNotInstanceOf(ByteBufferObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		listOperations.rightPushAll(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();

		listOperations.set(key, 1, value1).as(StepVerifier::create).expectNext(true).verifyComplete();

		listOperations.range(key, 0, -1) //
				.as(StepVerifier::create) //
				.expectNext(value1) //
				.expectNext(value1) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void remove() {

		assumeThat(this.valueFactory).isNotInstanceOf(ByteBufferObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		listOperations.rightPushAll(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();

		listOperations.remove(key, 1, value1) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

		listOperations.range(key, 0, -1) //
				.as(StepVerifier::create) //
				.expectNext(value2) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void index() {

		assumeThat(this.valueFactory).isNotInstanceOf(ByteBufferObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		listOperations.rightPushAll(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();

		listOperations.index(key, 1).as(StepVerifier::create).expectNext(value2).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2937
	void getFirst() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		listOperations.rightPushAll(key, v1, v2, v3).as(StepVerifier::create).expectNext(3L).verifyComplete();

		listOperations.getFirst(key).as(StepVerifier::create).expectNext(v1).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2937
	void getLast() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		listOperations.rightPushAll(key, v1, v2, v3).as(StepVerifier::create).expectNext(3L).verifyComplete();

		listOperations.getLast(key).as(StepVerifier::create).expectNext(v3).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-1196
	@EnabledOnCommand("LPOS")
	void indexOf() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		listOperations.rightPushAll(key, v1, v2, v1, v3).as(StepVerifier::create).expectNext(4L).verifyComplete();

		listOperations.indexOf(key, v1).as(StepVerifier::create).expectNext(0L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-1196
	@EnabledOnCommand("LPOS")
	void lastIndexOf() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		listOperations.rightPushAll(key, v1, v2, v1, v3).as(StepVerifier::create).expectNext(4L).verifyComplete();

		listOperations.lastIndexOf(key, v1).as(StepVerifier::create).expectNext(2L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void leftPop() {

		assumeThat(this.valueFactory).isNotInstanceOf(ByteBufferObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		listOperations.leftPushAll(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();

		listOperations.leftPop(key).as(StepVerifier::create).expectNext(value2).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2692
	void leftPopWithCount() {

		assumeThat(this.valueFactory).isNotInstanceOf(ByteBufferObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		listOperations.leftPushAll(key, value1, value2, value3).as(StepVerifier::create).expectNext(3L).verifyComplete();

		listOperations.leftPop(key, 2).as(StepVerifier::create).expectNext(value3).expectNext(value2).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void rightPop() {

		assumeThat(this.valueFactory).isNotInstanceOf(ByteBufferObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		listOperations.rightPushAll(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();

		listOperations.rightPop(key).as(StepVerifier::create).expectNext(value2).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2692
	void rightPopWithCount() {

		assumeThat(this.valueFactory).isInstanceOf(ByteBufferObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		listOperations.rightPushAll(key, value3, value2, value1).as(StepVerifier::create).expectNext(3L).verifyComplete();

		listOperations.rightPop(key, 2).as(StepVerifier::create).expectNext(value1).expectNext(value2).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void leftPopWithTimeout() {

		assumeThat(this.valueFactory).isNotInstanceOf(ByteBufferObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		listOperations.leftPushAll(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();

		listOperations.leftPop(key, Duration.ZERO).as(StepVerifier::create).expectNext(value2).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602, GH-2975
	void leftPopWithMillisecondTimeoutShouldBeFine() {

		assumeThat(this.valueFactory).isNotInstanceOf(ByteBufferObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		listOperations.leftPushAll(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();

		listOperations.leftPop(key, Duration.ofMillis(750)).as(StepVerifier::create).expectNext(value2).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void rightPopWithTimeout() {

		assumeThat(this.valueFactory).isNotInstanceOf(ByteBufferObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		listOperations.rightPushAll(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();

		listOperations.rightPop(key, Duration.ZERO).as(StepVerifier::create).expectNext(value2).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void rightPopAndLeftPush() {

		assumeThat(this.valueFactory).isNotInstanceOf(ByteBufferObjectFactory.class);

		K source = keyFactory.instance();
		K target = keyFactory.instance();
		V value = valueFactory.instance();

		listOperations.rightPush(source, value).as(StepVerifier::create).expectNext(1L).verifyComplete();

		listOperations.rightPopAndLeftPush(source, target).as(StepVerifier::create).expectNext(value).verifyComplete();

		listOperations.size(source).as(StepVerifier::create).expectNext(0L).verifyComplete();
		listOperations.size(target).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	@EnabledIfLongRunningTest
	void rightPopAndLeftPushWithTimeout() {

		assumeThat(this.valueFactory).isNotInstanceOf(ByteBufferObjectFactory.class);

		K source = keyFactory.instance();
		K target = keyFactory.instance();
		V value = valueFactory.instance();

		listOperations.rightPopAndLeftPush(source, target, Duration.ofSeconds(1)).as(StepVerifier::create).expectComplete()
				.verify();

		listOperations.rightPush(source, value).as(StepVerifier::create).expectNext(1L).verifyComplete();

		listOperations.rightPopAndLeftPush(source, target, Duration.ZERO).as(StepVerifier::create).expectNext(value)
				.verifyComplete();

		listOperations.size(source).as(StepVerifier::create).expectNext(0L).verifyComplete();
		listOperations.size(target).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void delete() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();

		listOperations.rightPush(key, value1).as(StepVerifier::create).expectNext(1L).verifyComplete();
		listOperations.delete(key).as(StepVerifier::create).expectNext(true).verifyComplete();

		listOperations.size(key).as(StepVerifier::create).expectNext(0L).verifyComplete();
	}
}
