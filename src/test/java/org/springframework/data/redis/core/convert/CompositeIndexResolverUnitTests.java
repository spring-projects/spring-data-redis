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
package org.springframework.data.redis.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.util.TypeInformation;

/**
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
class CompositeIndexResolverUnitTests {

	@Mock IndexResolver resolver1;
	@Mock IndexResolver resolver2;
	@Mock TypeInformation<?> typeInfoMock;

	@Test // DATAREDIS-425
	void shouldRejectNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> new CompositeIndexResolver(null));
	}

	@Test // DATAREDIS-425
	void shouldRejectCollectionWithNullValues() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new CompositeIndexResolver(Arrays.asList(resolver1, null, resolver2)));
	}

	@Test // DATAREDIS-425
	void shouldCollectionIndexesFromResolvers() {

		when(resolver1.resolveIndexesFor(any(TypeInformation.class), any()))
				.thenReturn(Collections.<IndexedData> singleton(new SimpleIndexedPropertyValue("spring", "data", "redis")));
		when(resolver2.resolveIndexesFor(any(TypeInformation.class), any()))
				.thenReturn(Collections.<IndexedData> singleton(new SimpleIndexedPropertyValue("redis", "data", "spring")));

		CompositeIndexResolver resolver = new CompositeIndexResolver(Arrays.asList(resolver1, resolver2));

		assertThat(resolver.resolveIndexesFor(typeInfoMock, "o.O").size()).isEqualTo(2);
	}
}
