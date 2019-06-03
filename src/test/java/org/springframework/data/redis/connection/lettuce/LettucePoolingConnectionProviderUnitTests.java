/*
 * Copyright 2019 the original author or authors.
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

import static org.mockito.Mockito.*;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Unit tests for {@link LettucePoolingConnectionProvider}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class LettucePoolingConnectionProviderUnitTests {

	@Mock LettuceConnectionProvider connectionProviderMock;
	@Mock StatefulRedisConnection<byte[], byte[]> connectionMock;
	@Mock RedisAsyncCommands<byte[], byte[]> commandsMock;

	LettucePoolingClientConfiguration config = LettucePoolingClientConfiguration.defaultConfiguration();

	@Before
	public void before() {

		when(connectionMock.async()).thenReturn(commandsMock);
		when(connectionProviderMock.getConnection(any())).thenReturn(connectionMock);
	}

	@Test // DATAREDIS-988
	public void shouldReturnConnectionOnRelease() {

		LettucePoolingConnectionProvider provider = new LettucePoolingConnectionProvider(connectionProviderMock, config);

		provider.release(provider.getConnection(StatefulRedisConnection.class));

		verifyZeroInteractions(commandsMock);
	}

	@Test // DATAREDIS-988
	public void shouldDiscardTransactionOnReleaseOnActiveTransaction() {

		LettucePoolingConnectionProvider provider = new LettucePoolingConnectionProvider(connectionProviderMock, config);
		when(connectionMock.isMulti()).thenReturn(true);

		provider.release(provider.getConnection(StatefulRedisConnection.class));

		verify(commandsMock).discard();
	}
}
