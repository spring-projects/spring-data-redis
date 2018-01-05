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
package org.springframework.data.redis.core.convert;

import static org.hamcrest.core.IsEqual.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.util.TypeInformation;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class CompositeIndexResolverUnitTests {

	@Mock IndexResolver resolver1;
	@Mock IndexResolver resolver2;
	@Mock TypeInformation<?> typeInfoMock;

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-425
	public void shouldRejectNull() {
		new CompositeIndexResolver(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-425
	public void shouldRejectCollectionWithNullValues() {
		new CompositeIndexResolver(Arrays.asList(resolver1, null, resolver2));
	}

	@Test // DATAREDIS-425
	public void shouldCollectionIndexesFromResolvers() {

		when(resolver1.resolveIndexesFor(any(TypeInformation.class), any())).thenReturn(
				Collections.<IndexedData> singleton(new SimpleIndexedPropertyValue("spring", "data", "redis")));
		when(resolver2.resolveIndexesFor(any(TypeInformation.class), any())).thenReturn(
				Collections.<IndexedData> singleton(new SimpleIndexedPropertyValue("redis", "data", "spring")));

		CompositeIndexResolver resolver = new CompositeIndexResolver(Arrays.asList(resolver1, resolver2));

		assertThat(resolver.resolveIndexesFor(typeInfoMock, "o.O").size(), equalTo(2));
	}
}
