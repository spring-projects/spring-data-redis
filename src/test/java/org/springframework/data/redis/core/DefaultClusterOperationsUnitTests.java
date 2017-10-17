/*
 * Copyright 2015-2017 the original author or authors.
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
package org.springframework.data.redis.core;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.redis.connection.RedisClusterCommands.AddSlots;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisServerCommands.MigrateOption;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultClusterOperationsUnitTests {

	static final RedisClusterNode NODE_1 = RedisClusterNode.newRedisClusterNode().listeningAt("127.0.0.1", 6379)
			.withId("d1861060fe6a534d42d8a19aeb36600e18785e04").build();

	static final RedisClusterNode NODE_2 = RedisClusterNode.newRedisClusterNode().listeningAt("127.0.0.1", 6380)
			.withId("0f2ee5df45d18c50aca07228cc18b1da96fd5e84").build();

	@Mock RedisConnectionFactory connectionFactory;
	@Mock RedisClusterConnection connection;

	RedisSerializer<String> serializer;

	DefaultClusterOperations<String, String> clusterOps;

	@Before
	public void setUp() {

		when(connectionFactory.getConnection()).thenReturn(connection);
		when(connectionFactory.getClusterConnection()).thenReturn(connection);

		serializer = StringRedisSerializer.UTF_8;

		RedisTemplate<String, String> template = new RedisTemplate<>();
		template.setConnectionFactory(connectionFactory);
		template.setValueSerializer(serializer);
		template.setKeySerializer(serializer);
		template.afterPropertiesSet();

		this.clusterOps = new DefaultClusterOperations<>(template);
	}

	@Test // DATAREDIS-315
	public void keysShouldDelegateToConnectionCorrectly() {

		Set<byte[]> keys = new HashSet<>(Arrays.asList(serializer.serialize("key-1"), serializer.serialize("key-2")));
		when(connection.keys(any(RedisClusterNode.class), any(byte[].class))).thenReturn(keys);

		assertThat(clusterOps.keys(NODE_1, "*"), hasItems("key-1", "key-2"));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void keysShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.keys(null, "*");
	}

	@Test // DATAREDIS-315
	public void keysShouldReturnEmptySetWhenNoKeysAvailable() {

		when(connection.keys(any(RedisClusterNode.class), any(byte[].class))).thenReturn(null);

		assertThat(clusterOps.keys(NODE_1, "*"), notNullValue());
	}

	@Test // DATAREDIS-315
	public void randomKeyShouldDelegateToConnection() {

		when(connection.randomKey(any(RedisClusterNode.class))).thenReturn(serializer.serialize("key-1"));

		assertThat(clusterOps.randomKey(NODE_1), is("key-1"));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void randomKeyShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.randomKey(null);
	}

	@Test // DATAREDIS-315
	public void randomKeyShouldReturnNullWhenNoKeyAvailable() {

		when(connection.randomKey(any(RedisClusterNode.class))).thenReturn(null);

		assertThat(clusterOps.randomKey(NODE_1), nullValue());
	}

	@Test // DATAREDIS-315
	public void pingShouldDelegateToConnection() {

		when(connection.ping(any(RedisClusterNode.class))).thenReturn("PONG");

		assertThat(clusterOps.ping(NODE_1), is("PONG"));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void pingShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.ping(null);
	}

	@Test // DATAREDIS-315
	public void addSlotsShouldDelegateToConnection() {

		clusterOps.addSlots(NODE_1, 1, 2, 3);

		verify(connection, times(1)).clusterAddSlots(eq(NODE_1), Mockito.<int[]> any());
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void addSlotsShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.addSlots(null);
	}

	@Test // DATAREDIS-315
	public void addSlotsWithRangeShouldDelegateToConnection() {

		clusterOps.addSlots(NODE_1, new SlotRange(1, 3));

		verify(connection, times(1)).clusterAddSlots(eq(NODE_1), Mockito.<int[]> any());
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void addSlotsWithRangeShouldThrowExceptionWhenRangeIsNull() {
		clusterOps.addSlots(NODE_1, (SlotRange) null);
	}

	@Test // DATAREDIS-315
	public void bgSaveShouldDelegateToConnection() {

		clusterOps.bgSave(NODE_1);

		verify(connection, times(1)).bgSave(eq(NODE_1));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void bgSaveShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.bgSave(null);
	}

	@Test // DATAREDIS-315
	public void meetShouldDelegateToConnection() {

		clusterOps.meet(NODE_1);

		verify(connection, times(1)).clusterMeet(eq(NODE_1));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void meetShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.meet(null);
	}

	@Test // DATAREDIS-315
	public void forgetShouldDelegateToConnection() {

		clusterOps.forget(NODE_1);

		verify(connection, times(1)).clusterForget(eq(NODE_1));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void forgetShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.forget(null);
	}

	@Test // DATAREDIS-315
	public void flushDbShouldDelegateToConnection() {

		clusterOps.flushDb(NODE_1);

		verify(connection, times(1)).flushDb(eq(NODE_1));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void flushDbShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.flushDb(null);
	}

	@Test // DATAREDIS-315
	public void getSlavesShouldDelegateToConnection() {

		clusterOps.getSlaves(NODE_1);

		verify(connection, times(1)).clusterGetSlaves(eq(NODE_1));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void getSlavesShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.getSlaves(null);
	}

	@Test // DATAREDIS-315
	public void saveShouldDelegateToConnection() {

		clusterOps.save(NODE_1);

		verify(connection, times(1)).save(eq(NODE_1));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void saveShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.save(null);
	}

	@Test // DATAREDIS-315
	public void shutdownShouldDelegateToConnection() {

		clusterOps.shutdown(NODE_1);

		verify(connection, times(1)).shutdown(eq(NODE_1));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void shutdownShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.shutdown(null);
	}

	@Test // DATAREDIS-315
	public void executeShouldDelegateToConnection() {

		final byte[] key = serializer.serialize("foo");
		clusterOps.execute(connection -> serializer.deserialize(connection.get(key)));

		verify(connection, times(1)).get(eq(key));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void executeShouldThrowExceptionWhenCallbackIsNull() {
		clusterOps.execute(null);
	}

	@Test // DATAREDIS-315
	public void reshardShouldExecuteCommandsCorrectly() {

		byte[] key = "foo".getBytes();
		when(connection.clusterGetKeysInSlot(eq(100), anyInt())).thenReturn(Collections.singletonList(key));
		clusterOps.reshard(NODE_1, 100, NODE_2);

		verify(connection, times(1)).clusterSetSlot(eq(NODE_2), eq(100), eq(AddSlots.IMPORTING));
		verify(connection, times(1)).clusterSetSlot(eq(NODE_1), eq(100), eq(AddSlots.MIGRATING));
		verify(connection, times(1)).clusterGetKeysInSlot(eq(100), anyInt());
		verify(connection, times(1)).migrate(any(byte[].class), eq(NODE_1), eq(0), eq(MigrateOption.COPY));
		verify(connection, times(1)).clusterSetSlot(eq(NODE_2), eq(100), eq(AddSlots.NODE));

	}
}
