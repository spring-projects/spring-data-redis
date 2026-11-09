/*
 * Copyright 2026 the original author or authors.
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
import static org.mockito.Mockito.*;
import static org.springframework.data.redis.connection.lettuce.LettuceReactiveCommandsTestSupport.*;

import io.lettuce.core.RedisException;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import org.springframework.data.redis.RedisSystemException;

/**
 * @author Sohyeon An
 */
class LettuceReactiveClusterKeyCommandsUnitTests {

	private final byte[] value = new byte[] { 1, 2, 3 };
	private final LettuceReactiveRedisClusterConnection connection = mock(LettuceReactiveRedisClusterConnection.class,
			CALLS_REAL_METHODS);
	@SuppressWarnings("unchecked") private final RedisClusterReactiveCommands<ByteBuffer, ByteBuffer> nativeCommands = mock(
			RedisClusterReactiveCommands.class);
	private final LettuceReactiveClusterKeyCommands commands = new LettuceReactiveClusterKeyCommands(connection);

	@BeforeEach
	void setUp() {

		doReturn(Mono.just(nativeCommands)).when(connection).getCommands();
		when(nativeCommands.dump(KEY_1_BBUFFER)).thenReturn(Mono.just(value));
		when(nativeCommands.exists(KEY_2_BBUFFER)).thenReturn(Mono.just(0L));
	}

	@ParameterizedTest // GH-3476
	@ValueSource(booleans = { false, true })
	void renameShouldKeepExpiringKeyExpiring(boolean nx) {

		when(nativeCommands.pttl(KEY_1_BBUFFER)).thenReturn(Mono.just(0L));
		when(nativeCommands.restore(eq(KEY_2_BBUFFER), eq(value), any(RestoreArgs.class))).thenReturn(Mono.just("OK"));
		when(nativeCommands.restore(KEY_2_BBUFFER, 1, value)).thenReturn(Mono.just("OK"));
		when(nativeCommands.del(KEY_1_BBUFFER)).thenReturn(Mono.just(1L));

		(nx ? commands.renameNX(KEY_1_BBUFFER, KEY_2_BBUFFER) : commands.rename(KEY_1_BBUFFER, KEY_2_BBUFFER))
				.as(StepVerifier::create).expectNext(true).verifyComplete();

		if (nx) {
			verify(nativeCommands).restore(KEY_2_BBUFFER, 1, value);
		} else {
			ArgumentCaptor<RestoreArgs> restoreArgs = ArgumentCaptor.forClass(RestoreArgs.class);
			verify(nativeCommands).restore(eq(KEY_2_BBUFFER), eq(value), restoreArgs.capture());
			assertThat(restoreArgs.getValue()).hasFieldOrPropertyWithValue("ttl", 1L).hasFieldOrPropertyWithValue("replace",
					true);
		}
		verify(nativeCommands).del(KEY_1_BBUFFER);
	}

	@ParameterizedTest // GH-3476
	@ValueSource(booleans = { false, true })
	void renameShouldNotRestoreKeyThatExpiredAfterDump(boolean nx) {

		when(nativeCommands.pttl(KEY_1_BBUFFER)).thenReturn(Mono.just(-2L));

		(nx ? commands.renameNX(KEY_1_BBUFFER, KEY_2_BBUFFER) : commands.rename(KEY_1_BBUFFER, KEY_2_BBUFFER))
				.as(StepVerifier::create).expectNext(false).verifyComplete();

		verify(nativeCommands, never()).restore(any(), anyLong(), any());
		verify(nativeCommands, never()).restore(any(), any(byte[].class), any(RestoreArgs.class));
		verify(nativeCommands, never()).del(KEY_1_BBUFFER);
	}

	@ParameterizedTest // GH-3476
	@ValueSource(booleans = { false, true })
	void renameShouldNotDeleteSourceWhenRestoreFails(boolean nx) {

		when(nativeCommands.pttl(KEY_1_BBUFFER)).thenReturn(Mono.just(60_000L));
		when(nativeCommands.restore(eq(KEY_2_BBUFFER), eq(value), any(RestoreArgs.class)))
				.thenReturn(Mono.error(new RedisException("RESTORE failed")));
		when(nativeCommands.restore(KEY_2_BBUFFER, 60_000, value))
				.thenReturn(Mono.error(new RedisException("RESTORE failed")));

		(nx ? commands.renameNX(KEY_1_BBUFFER, KEY_2_BBUFFER) : commands.rename(KEY_1_BBUFFER, KEY_2_BBUFFER))
				.as(StepVerifier::create).expectError(RedisSystemException.class).verify();

		verify(nativeCommands, never()).del(KEY_1_BBUFFER);
	}
}
