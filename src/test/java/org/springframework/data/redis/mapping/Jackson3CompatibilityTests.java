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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Disabled;
import org.springframework.data.redis.hash.Jackson2HashMapper;
import org.springframework.data.redis.hash.Jackson3HashMapper;

/**
 * @author Christoph Strobl
 */
public class Jackson3CompatibilityTests extends Jackson3HashMapperUnitTests {

	private final Jackson2HashMapper jackson2HashMapper;

	public Jackson3CompatibilityTests() {
		super(new Jackson3HashMapper(Jackson3HashMapper::preconfigure, false));
		this.jackson2HashMapper = new Jackson2HashMapper(false);
	}

	@Override
	@Disabled("with jackson 2 this used to render the timestamp as string. Now its a long and in line with calendar timestamp")
	void dateValueShouldBeTreatedCorrectly() {
		super.dateValueShouldBeTreatedCorrectly();
	}

	@Override
	@Disabled("with jackson 2 used to render the enum and its type hint in an array. Now its just the enum value")
	void enumsShouldBeTreatedCorrectly() {
		super.enumsShouldBeTreatedCorrectly();
	}

	@Override
	protected void assertBackAndForwardMapping(Object o) {

		Map<String, Object> hash3 = getMapper().toHash(o);
		Map<String, Object> hash2 = jackson2HashMapper.toHash(o);

		assertThat(hash3).containsAllEntriesOf(hash2);
		assertThat(getMapper().fromHash(hash2)).isEqualTo(o);
	}
}
