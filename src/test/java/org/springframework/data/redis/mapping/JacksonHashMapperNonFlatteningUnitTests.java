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

import java.time.LocalDateTime;
import java.time.Month;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.hash.JacksonHashMapper;

/**
 * Unit tests for {@link JacksonHashMapper} using hierarchical mode.
 *
 * @author Christoph Strobl
 * @author John Blum
 */
class JacksonHashMapperNonFlatteningUnitTests extends JacksonHashMapperUnitTests {

	JacksonHashMapperNonFlatteningUnitTests() {
		super(JacksonHashMapper.hierarchical());
	}

	@Test // GH-2593
	void timestampHandledCorrectly() {

		Session source = new Session();
		source.lastAccessed = LocalDateTime.of(2023, Month.JUNE, 5, 18, 36, 30);

		Map<String, Object> hash = getMapper().toHash(source);
		Session session = (Session) getMapper().fromHash(hash);

		assertThat(session).isNotNull();
		assertThat(session.lastAccessed).isEqualTo(LocalDateTime.of(2023, Month.JUNE, 5, 18, 36, 30));
	}

	private static class Session {

		private LocalDateTime lastAccessed;

		public LocalDateTime getLastAccessed() {
			return lastAccessed;
		}

		public void setLastAccessed(LocalDateTime lastAccessed) {
			this.lastAccessed = lastAccessed;
		}
	}
}
