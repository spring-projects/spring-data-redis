/*
 * Copyright 2015-2016 the original author or authors.
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

import static org.hamcrest.core.IsCollectionContaining.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedHashSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.convert.PathIndexResolver;
import org.springframework.data.redis.core.convert.IndexedData;
import org.springframework.data.redis.core.convert.MappingRedisConverter;
import org.springframework.data.redis.core.convert.ReferenceResolver;
import org.springframework.data.redis.core.convert.SimpleIndexedPropertyValue;
import org.springframework.data.redis.core.mapping.RedisMappingContext;

/**
 * @author Christoph Strobl
 * @auhtor Rob Winch
 */
@RunWith(MockitoJUnitRunner.class)
public class IndexWriterUnitTests {

	private static final Charset CHARSET = Charset.forName("UTF-8");
	private static final String KEYSPACE = "persons";
	private static final String KEY = "key-1";
	private static final byte[] KEY_BIN = KEY.getBytes(CHARSET);
	IndexWriter writer;

	@Mock RedisConnection connectionMock;
	@Mock ReferenceResolver referenceResolverMock;

	@Before
	public void setUp() {

		MappingRedisConverter converter = new MappingRedisConverter(new RedisMappingContext(), new PathIndexResolver(),
				referenceResolverMock);
		converter.afterPropertiesSet();

		writer = new IndexWriter(connectionMock, converter);
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void addKeyToIndexShouldInvokeSaddCorrectly() {

		writer.addKeyToIndex(KEY_BIN, new SimpleIndexedPropertyValue(KEYSPACE, "firstname", "Rand"));

		verify(connectionMock).sAdd(eq("persons:firstname:Rand".getBytes(CHARSET)), eq(KEY_BIN));
		verify(connectionMock).sAdd(eq("persons:key-1:idx".getBytes(CHARSET)),
				eq("persons:firstname:Rand".getBytes(CHARSET)));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test(expected = IllegalArgumentException.class)
	public void addKeyToIndexShouldThrowErrorWhenIndexedDataIsNull() {
		writer.addKeyToIndex(KEY_BIN, null);
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void removeKeyFromExistingIndexesShouldCheckForExistingIndexesForPath() {

		writer.removeKeyFromExistingIndexes(KEY_BIN, new StubIndxedData());

		verify(connectionMock).keys(eq(("persons:address.city:*").getBytes(CHARSET)));
		verifyNoMoreInteractions(connectionMock);
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void removeKeyFromExistingIndexesShouldRemoveKeyFromAllExistingIndexesForPath() {

		byte[] indexKey1 = "persons:firstname:rand".getBytes(CHARSET);
		byte[] indexKey2 = "persons:firstname:mat".getBytes(CHARSET);

		when(connectionMock.keys(any(byte[].class))).thenReturn(
				new LinkedHashSet<byte[]>(Arrays.asList(indexKey1, indexKey2)));

		writer.removeKeyFromExistingIndexes(KEY_BIN, new StubIndxedData());

		verify(connectionMock).sRem(indexKey1, KEY_BIN);
		verify(connectionMock).sRem(indexKey2, KEY_BIN);
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test(expected = IllegalArgumentException.class)
	public void removeKeyFromExistingIndexesShouldThrowExecptionForNullIndexedData() {
		writer.removeKeyFromExistingIndexes(KEY_BIN, null);
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void removeAllIndexesShouldDeleteAllIndexKeys() {

		byte[] indexKey1 = "persons:firstname:rand".getBytes(CHARSET);
		byte[] indexKey2 = "persons:firstname:mat".getBytes(CHARSET);

		when(connectionMock.keys(any(byte[].class))).thenReturn(
				new LinkedHashSet<byte[]>(Arrays.asList(indexKey1, indexKey2)));

		writer.removeAllIndexes(KEYSPACE);

		ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);

		verify(connectionMock, times(1)).del(captor.capture());
		assertThat(captor.getAllValues(), hasItems(indexKey1, indexKey2));
	}

	static class StubIndxedData implements IndexedData {

		@Override
		public String getIndexName() {
			return "address.city";
		}

		@Override
		public String getKeySpace() {
			return KEYSPACE;
		}

	}
}
