/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.redis.cache;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DefaultCacheStatistics}.
 *
 * @author Mark Paluch
 */
class DefaultCacheStatisticsUnitTests {

	DefaultCacheStatistics statistics = new DefaultCacheStatistics();

	@Test // DATAREDIS-1082
	void shouldReportRetrievals() {

		assertThat(statistics.getRetrievals()).isZero();

		statistics.incrementRetrievals();

		assertThat(statistics.getRetrievals()).isOne();
	}

	@Test // DATAREDIS-1082
	void shouldReportHits() {

		assertThat(statistics.getHits()).isZero();

		statistics.incrementHits();

		assertThat(statistics.getHits()).isOne();
	}

	@Test // DATAREDIS-1082
	void shouldReportMisses() {

		assertThat(statistics.getMisses()).isZero();

		statistics.incrementMisses();

		assertThat(statistics.getMisses()).isOne();
	}

	@Test // DATAREDIS-1082
	void shouldReportPuts() {

		assertThat(statistics.getStores()).isZero();

		statistics.incrementStores();

		assertThat(statistics.getStores()).isOne();
	}

	@Test // DATAREDIS-1082
	void shouldReportRemovals() {

		assertThat(statistics.getRemovals()).isZero();

		statistics.incrementRemovals();

		assertThat(statistics.getRemovals()).isOne();
	}
}
