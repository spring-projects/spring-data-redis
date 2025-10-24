/*
 * Copyright 2016-2025 the original author or authors.
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
package org.springframework.data.redis.core.mapping;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.keyvalue.core.mapping.KeySpaceResolver;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.redis.core.TimeToLiveAccessor;
import org.springframework.data.redis.core.convert.ConversionTestEntities;
import org.springframework.data.util.TypeInformation;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @param <T>
 */
@ExtendWith(MockitoExtension.class)
class BasicRedisPersistentEntityUnitTests<T> {

	@Mock TypeInformation<T> entityInformation;
	@Mock KeySpaceResolver keySpaceResolver;
	@Mock TimeToLiveAccessor ttlAccessor;

	private BasicRedisPersistentEntity<T> entity;

	@BeforeEach
	@SuppressWarnings("unchecked")
	void setUp() {

		when(entityInformation.getType()).thenReturn((Class<T>) ConversionTestEntities.Person.class);
		entity = new BasicRedisPersistentEntity<>(entityInformation, keySpaceResolver, ttlAccessor);
	}

	@Test // DATAREDIS-425
	void addingMultipleIdPropertiesWithoutAnExplicitOneThrowsException() {

		RedisPersistentProperty property1 = mock(RedisPersistentProperty.class);
		when(property1.isIdProperty()).thenReturn(true);

		RedisPersistentProperty property2 = mock(RedisPersistentProperty.class);
		when(property2.isIdProperty()).thenReturn(true);

		entity.addPersistentProperty(property1);

		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> entity.addPersistentProperty(property2))
				.withMessageContaining("Attempt to add id property").withMessageContaining("but already have a property");
	}

	@Test // DATAREDIS-425
	@SuppressWarnings("unchecked")
	void addingMultipleExplicitIdPropertiesThrowsException() {

		RedisPersistentProperty property1 = mock(RedisPersistentProperty.class);
		when(property1.isIdProperty()).thenReturn(true);
		when(property1.isAnnotationPresent(any(Class.class))).thenReturn(true);

		RedisPersistentProperty property2 = mock(RedisPersistentProperty.class);
		when(property2.isIdProperty()).thenReturn(true);
		when(property2.isAnnotationPresent(any(Class.class))).thenReturn(true);

		entity.addPersistentProperty(property1);
		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> entity.addPersistentProperty(property2))
				.withMessageContaining("Attempt to add explicit id property")
				.withMessageContaining("but already have a property");
	}

	@Test // DATAREDIS-425
	@SuppressWarnings("unchecked")
	void explicitIdPropertiyShouldBeFavoredOverNonExplicit() {

		RedisPersistentProperty property1 = mock(RedisPersistentProperty.class);
		when(property1.isIdProperty()).thenReturn(true);

		RedisPersistentProperty property2 = mock(RedisPersistentProperty.class);
		when(property2.isIdProperty()).thenReturn(true);
		when(property2.isAnnotationPresent(any(Class.class))).thenReturn(true);

		entity.addPersistentProperty(property1);
		entity.addPersistentProperty(property2);

		assertThat(entity.getIdProperty()).isEqualTo(property2);
	}
}
