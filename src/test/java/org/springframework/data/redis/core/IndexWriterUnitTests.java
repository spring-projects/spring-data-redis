/*
 * Copyright 2015-2018 the original author or authors.
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
import static org.mockito.Mockito.*;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.convert.GeoIndexedPropertyValue;
import org.springframework.data.redis.core.convert.IndexedData;
import org.springframework.data.redis.core.convert.MappingRedisConverter;
import org.springframework.data.redis.core.convert.PathIndexResolver;
import org.springframework.data.redis.core.convert.ReferenceResolver;
import org.springframework.data.redis.core.convert.SimpleIndexedPropertyValue;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @author Rob Winch
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class IndexWriterUnitTests {

	private static final Charset CHARSET = Charset.forName("UTF-8");
	private static final String KEYSPACE = "persons";
	private static final String KEY = "key-1";
	private static final byte[] KEY_BIN = KEY.getBytes(CHARSET);
	IndexWriter writer;
	MappingRedisConverter converter;

	@Mock RedisConnection connectionMock;
	@Mock ReferenceResolver referenceResolverMock;

	@Before
	public void setUp() {

		converter = new MappingRedisConverter(new RedisMappingContext(), new PathIndexResolver(), referenceResolverMock);
		converter.afterPropertiesSet();

		writer = new IndexWriter(connectionMock, converter);
	}

	@Test // DATAREDIS-425
	public void addKeyToIndexShouldInvokeSaddCorrectly() {

		writer.addKeyToIndex(KEY_BIN, new SimpleIndexedPropertyValue(KEYSPACE, "firstname", "Rand"));

		verify(connectionMock).sAdd(eq("persons:firstname:Rand".getBytes(CHARSET)), eq(KEY_BIN));
		verify(connectionMock).sAdd(eq("persons:key-1:idx".getBytes(CHARSET)),
				eq("persons:firstname:Rand".getBytes(CHARSET)));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-425
	public void addKeyToIndexShouldThrowErrorWhenIndexedDataIsNull() {
		writer.addKeyToIndex(KEY_BIN, null);
	}

	@Test // DATAREDIS-425
	public void removeKeyFromExistingIndexesShouldCheckForExistingIndexesForPath() {

		writer.removeKeyFromExistingIndexes(KEY_BIN, new StubIndxedData());

		verify(connectionMock).keys(eq(("persons:address.city:*").getBytes(CHARSET)));
		verifyNoMoreInteractions(connectionMock);
	}

	@Test // DATAREDIS-425
	public void removeKeyFromExistingIndexesShouldRemoveKeyFromAllExistingIndexesForPath() {

		byte[] indexKey1 = "persons:firstname:rand".getBytes(CHARSET);
		byte[] indexKey2 = "persons:firstname:mat".getBytes(CHARSET);

		when(connectionMock.keys(any(byte[].class)))
				.thenReturn(new LinkedHashSet<>(Arrays.asList(indexKey1, indexKey2)));

		writer.removeKeyFromExistingIndexes(KEY_BIN, new StubIndxedData());

		verify(connectionMock).sRem(indexKey1, KEY_BIN);
		verify(connectionMock).sRem(indexKey2, KEY_BIN);
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-425
	public void removeKeyFromExistingIndexesShouldThrowExecptionForNullIndexedData() {
		writer.removeKeyFromExistingIndexes(KEY_BIN, null);
	}

	@Test // DATAREDIS-425
	public void removeAllIndexesShouldDeleteAllIndexKeys() {

		byte[] indexKey1 = "persons:firstname:rand".getBytes(CHARSET);
		byte[] indexKey2 = "persons:firstname:mat".getBytes(CHARSET);

		when(connectionMock.keys(any(byte[].class)))
				.thenReturn(new LinkedHashSet<>(Arrays.asList(indexKey1, indexKey2)));

		writer.removeAllIndexes(KEYSPACE);

		ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);

		verify(connectionMock, times(1)).del(captor.capture());
		assertThat(captor.getAllValues(), hasItems(indexKey1, indexKey2));
	}

	@Test(expected = InvalidDataAccessApiUsageException.class) // DATAREDIS-425
	public void addToIndexShouldThrowDataAccessExceptionWhenAddingDataThatConnotBeConverted() {
		writer.addKeyToIndex(KEY_BIN, new SimpleIndexedPropertyValue(KEYSPACE, "firstname", new DummyObject()));

	}

	@Test // DATAREDIS-425
	public void addToIndexShouldUseRegisteredConverterWhenAddingData() {

		DummyObject value = new DummyObject();
		final String identityHexString = ObjectUtils.getIdentityHexString(value);

		((GenericConversionService) converter.getConversionService()).addConverter(new Converter<DummyObject, byte[]>() {

			@Override
			public byte[] convert(DummyObject source) {
				return identityHexString.getBytes(CHARSET);
			}
		});

		writer.addKeyToIndex(KEY_BIN, new SimpleIndexedPropertyValue(KEYSPACE, "firstname", value));

		verify(connectionMock).sAdd(eq(("persons:firstname:" + identityHexString).getBytes(CHARSET)), eq(KEY_BIN));
	}

	@Test // DATAREDIS-512
	public void createIndexShouldNotTryToRemoveExistingValues() {

		when(connectionMock.keys(any(byte[].class)))
				.thenReturn(new LinkedHashSet<>(Arrays.asList("persons:firstname:rand".getBytes(CHARSET))));

		writer.createIndexes(KEY_BIN,
				Collections.<IndexedData> singleton(new SimpleIndexedPropertyValue(KEYSPACE, "firstname", "Rand")));

		verify(connectionMock).sAdd(eq("persons:firstname:Rand".getBytes(CHARSET)), eq(KEY_BIN));
		verify(connectionMock).sAdd(eq("persons:key-1:idx".getBytes(CHARSET)),
				eq("persons:firstname:Rand".getBytes(CHARSET)));
		verify(connectionMock, never()).sRem(any(byte[].class), eq(KEY_BIN));
	}

	@Test // DATAREDIS-512
	public void updateIndexShouldRemoveExistingValues() {

		when(connectionMock.keys(any(byte[].class)))
				.thenReturn(new LinkedHashSet<>(Arrays.asList("persons:firstname:rand".getBytes(CHARSET))));

		writer.updateIndexes(KEY_BIN,
				Collections.<IndexedData> singleton(new SimpleIndexedPropertyValue(KEYSPACE, "firstname", "Rand")));

		verify(connectionMock).sAdd(eq("persons:firstname:Rand".getBytes(CHARSET)), eq(KEY_BIN));
		verify(connectionMock).sAdd(eq("persons:key-1:idx".getBytes(CHARSET)),
				eq("persons:firstname:Rand".getBytes(CHARSET)));
		verify(connectionMock, times(1)).sRem(any(byte[].class), eq(KEY_BIN));
	}

	@Test // DATAREDIS-533
	public void removeGeoIndexShouldCallGeoRemove() {

		byte[] indexKey1 = "persons:location".getBytes(CHARSET);

		when(connectionMock.keys(any(byte[].class))).thenReturn(new LinkedHashSet<>(Arrays.asList(indexKey1)));

		writer.removeKeyFromExistingIndexes(KEY_BIN, new GeoIndexedPropertyValue(KEYSPACE, "address.city", null));

		verify(connectionMock).geoRemove(indexKey1, KEY_BIN);
	}

	static class StubIndxedData implements IndexedData {

		@Override
		public String getIndexName() {
			return "address.city";
		}

		@Override
		public String getKeyspace() {
			return KEYSPACE;
		}
	}

	static class DummyObject {

	}
}
