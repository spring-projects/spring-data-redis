/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.redis.mapping;

import static org.assertj.core.api.Assertions.*;

import java.util.Map;

import org.springframework.data.redis.hash.Jackson2HashMapper;
import org.springframework.data.redis.hash.Jackson3HashMapper;

/**
 * TCK-style tests to assert {@link Jackson3HashMapper} interoperability with {@link Jackson2HashMapper} in flattening
 * mode.
 *
 * @author Christoph Strobl
 */
@SuppressWarnings("removal")
class Jackson3FlatteningCompatibilityTests extends Jackson3HashMapperUnitTests {

	private final Jackson2HashMapper jackson2HashMapper;

	Jackson3FlatteningCompatibilityTests() {
		super(Jackson3HashMapper.builder().jackson2CompatibilityMode().flatten().build());
		this.jackson2HashMapper = new Jackson2HashMapper(true);
	}

	@Override
	protected void assertBackAndForwardMapping(Object o) {

		Map<String, Object> hash3 = getMapper().toHash(o);
		Map<String, Object> hash2 = jackson2HashMapper.toHash(o);

		assertThat(hash3).containsAllEntriesOf(hash2);
		assertThat(getMapper().fromHash(hash2)).isEqualTo(o);
	}
}
