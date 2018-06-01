/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.redis.repository.core;

import static org.mockito.Mockito.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.redis.core.mapping.RedisPersistentEntity;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class MappingRedisEntityInformationUnitTests<T, ID> {

	@Mock RedisPersistentEntity<T> entity;

	@Test(expected = MappingException.class) // DATAREDIS-425
	public void throwsMappingExceptionWhenNoIdPropertyPresent() {

		when(entity.hasIdProperty()).thenReturn(false);

		new MappingRedisEntityInformation<T, ID>(entity);
	}
}
