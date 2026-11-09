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
package org.springframework.data.redis.connection.jedis;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * @author Sohyeon An
 */
class JedisClusterKeyCommandsUnitTests {

	private final byte[] source = "key1".getBytes(java.nio.charset.StandardCharsets.UTF_8);
	private final byte[] target = "key2".getBytes(java.nio.charset.StandardCharsets.UTF_8);
	private final byte[] value = new byte[] { 1, 2, 3 };
	private final JedisClusterKeyCommands commands = spy(new JedisClusterKeyCommands(mock(JedisClusterConnection.class)));

	@ParameterizedTest // GH-3476
	@ValueSource(booleans = { false, true })
	void renameShouldKeepExpiringKeyExpiring(boolean nx) {

		doReturn(value).when(commands).dump(source);
		doReturn(0L).when(commands).pTtl(source);
		doReturn(false).when(commands).exists(target);
		doReturn(1L).when(commands).del(source);
		doNothing().when(commands).restore(target, 1, value, true);
		doNothing().when(commands).restore(target, 1, value);

		if (nx) {
			assertThat(commands.renameNX(source, target)).isTrue();
			verify(commands).restore(target, 1, value);
		} else {
			commands.rename(source, target);
			verify(commands).restore(target, 1, value, true);
		}
		verify(commands).del(source);
	}

	@ParameterizedTest // GH-3476
	@ValueSource(booleans = { false, true })
	void renameShouldNotRestoreKeyThatExpiredAfterDump(boolean nx) {

		doReturn(value).when(commands).dump(source);
		doReturn(-2L).when(commands).pTtl(source);
		doReturn(false).when(commands).exists(target);

		if (nx) {
			assertThat(commands.renameNX(source, target)).isFalse();
		} else {
			commands.rename(source, target);
		}

		verify(commands, never()).restore(any(), anyLong(), any());
		verify(commands, never()).restore(any(), anyLong(), any(), anyBoolean());
		verify(commands, never()).del(source);
	}
}
