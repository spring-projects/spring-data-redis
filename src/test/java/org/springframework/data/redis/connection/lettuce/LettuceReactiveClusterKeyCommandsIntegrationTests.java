/*
 * Copyright 2016-present the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.redis.connection.RedisClusterNode.*;
import static org.springframework.data.redis.connection.lettuce.LettuceReactiveCommandsTestSupport.*;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisClusterNode;

/**
 * @author Christoph Strobl
 * @author Sohyeon An
 */
class LettuceReactiveClusterKeyCommandsIntegrationTests extends LettuceReactiveClusterTestSupport {

	private static final RedisClusterNode NODE_1 = newRedisClusterNode().listeningAt("127.0.0.1", 7379).build();

	@Test // DATAREDIS-525
	void keysShouldReturnOnlyKeysFromSelectedNode() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		List<ByteBuffer> result = connection.keyCommands().keys(NODE_1, ByteBuffer.wrap("*".getBytes())).block();
		assertThat(result).hasSize(1);
		assertThat(result).containsExactly(KEY_1_BBUFFER);
	}

	@Test // DATAREDIS-525
	void randomkeyShouldReturnOnlyKeysFromSelectedNode() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Mono<ByteBuffer> randomkey = connection.keyCommands().randomKey(NODE_1);

		for (int i = 0; i < 10; i++) {
			assertThat(randomkey.block()).isEqualTo(KEY_1_BBUFFER);
		}
	}

	@ParameterizedTest // GH-3476
	@ValueSource(booleans = { false, true })
	void renameAcrossSlotsShouldPreserveTimeToLive(boolean nx) {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.pexpire(KEY_1, 60_000);
		Long sourceTtl = nativeCommands.pttl(KEY_1);

		(nx ? connection.keyCommands().renameNX(KEY_1_BBUFFER, KEY_2_BBUFFER)
				: connection.keyCommands().rename(KEY_1_BBUFFER, KEY_2_BBUFFER)).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		assertThat(nativeCommands.exists(KEY_1)).isZero();
		assertThat(nativeCommands.get(KEY_2)).isEqualTo(VALUE_1);
		assertThat(nativeCommands.pttl(KEY_2)).isPositive().isLessThanOrEqualTo(sourceTtl);
	}

	@ParameterizedTest // GH-3476
	@ValueSource(booleans = { false, true })
	void renameAcrossSlotsShouldKeepPersistentKey(boolean nx) {

		nativeCommands.set(KEY_1, VALUE_1);

		(nx ? connection.keyCommands().renameNX(KEY_1_BBUFFER, KEY_2_BBUFFER)
				: connection.keyCommands().rename(KEY_1_BBUFFER, KEY_2_BBUFFER)).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		assertThat(nativeCommands.exists(KEY_1)).isZero();
		assertThat(nativeCommands.get(KEY_2)).isEqualTo(VALUE_1);
		assertThat(nativeCommands.pttl(KEY_2)).isEqualTo(-1L);
	}

	@ParameterizedTest // GH-3476
	@ValueSource(booleans = { false, true })
	void renameShouldReplaceTargetTimeToLive(boolean persistent) {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);
		nativeCommands.pexpire(KEY_2, 120_000);
		if (!persistent) {
			nativeCommands.pexpire(KEY_1, 60_000);
		}
		Long sourceTtl = nativeCommands.pttl(KEY_1);

		connection.keyCommands().rename(KEY_1_BBUFFER, KEY_2_BBUFFER).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		assertThat(nativeCommands.exists(KEY_1)).isZero();
		assertThat(nativeCommands.get(KEY_2)).isEqualTo(VALUE_1);
		if (persistent) {
			assertThat(nativeCommands.pttl(KEY_2)).isEqualTo(-1L);
		} else {
			assertThat(nativeCommands.pttl(KEY_2)).isPositive().isLessThanOrEqualTo(sourceTtl);
		}
	}

	@Test // GH-3476
	void renameNXShouldKeepExistingTarget() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.pexpire(KEY_1, 60_000);
		nativeCommands.set(KEY_2, VALUE_2);
		nativeCommands.pexpire(KEY_2, 120_000);

		connection.keyCommands().renameNX(KEY_1_BBUFFER, KEY_2_BBUFFER).as(StepVerifier::create).expectNext(false)
				.verifyComplete();

		assertThat(nativeCommands.get(KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeCommands.get(KEY_2)).isEqualTo(VALUE_2);
		assertThat(nativeCommands.pttl(KEY_1)).isPositive().isLessThanOrEqualTo(60_000);
		assertThat(nativeCommands.pttl(KEY_2)).isGreaterThan(60_000).isLessThanOrEqualTo(120_000);
	}

	@ParameterizedTest // GH-3476
	@ValueSource(booleans = { false, true })
	void renameOnSameSlotShouldPreserveTimeToLive(boolean nx) {

		nativeCommands.set(SAME_SLOT_KEY_1, VALUE_1);
		nativeCommands.pexpire(SAME_SLOT_KEY_1, 60_000);
		Long sourceTtl = nativeCommands.pttl(SAME_SLOT_KEY_1);

		(nx ? connection.keyCommands().renameNX(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER)
				: connection.keyCommands().rename(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER)).as(StepVerifier::create)
				.expectNext(true).verifyComplete();

		assertThat(nativeCommands.exists(SAME_SLOT_KEY_1)).isZero();
		assertThat(nativeCommands.get(SAME_SLOT_KEY_2)).isEqualTo(VALUE_1);
		assertThat(nativeCommands.pttl(SAME_SLOT_KEY_2)).isPositive().isLessThanOrEqualTo(sourceTtl);
	}

	@ParameterizedTest // GH-3476
	@ValueSource(booleans = { false, true })
	void renameShouldFailWhenSourceDoesNotExist(boolean nx) {

		(nx ? connection.keyCommands().renameNX(KEY_1_BBUFFER, KEY_2_BBUFFER)
				: connection.keyCommands().rename(KEY_1_BBUFFER, KEY_2_BBUFFER)).as(StepVerifier::create)
				.expectError(RedisSystemException.class).verify();
	}

}
