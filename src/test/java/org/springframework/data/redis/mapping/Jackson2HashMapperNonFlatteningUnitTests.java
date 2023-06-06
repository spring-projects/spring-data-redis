/*
 * Copyright 2023 the original author or authors.
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

import java.time.LocalDateTime;
import java.time.Month;
import java.util.Arrays;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.hash.Jackson2HashMapper;

import lombok.Data;

/**
 * @author Christoph Strobl
 * @author John Blum
 * @since 2023/06
 */
public class Jackson2HashMapperNonFlatteningUnitTests extends Jackson2HashMapperUnitTests {

	Jackson2HashMapperNonFlatteningUnitTests() {
		super(new Jackson2HashMapper(false));
	}

	@Test // GH-2593
	void timestampHandledCorrectly() {

		Map<String, Object> hash = Map.of(
			"@class", Session.class.getName(),
			"lastAccessed", Arrays.asList(LocalDateTime.class.getName(), "2023-06-05T18:36:30")
		);

		Session session = (Session) getMapper().fromHash(hash);

		assertThat(session).isNotNull();
		assertThat(session.getLastAccessed()).isEqualTo(LocalDateTime.of(2023, Month.JUNE, 5,
			18, 36, 30));
	}

	@Data
	private static class Session {
		private LocalDateTime lastAccessed;
	}
}
