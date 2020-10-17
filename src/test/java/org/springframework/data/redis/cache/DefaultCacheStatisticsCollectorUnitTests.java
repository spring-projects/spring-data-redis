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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DefaultCacheStatisticsCollector}.
 *
 * @author Christoph Strobl
 */
class DefaultCacheStatisticsCollectorUnitTests {

	static final String CACHE_1 = "cache:1";
	static final String CACHE_2 = "cache:2";

	DefaultCacheStatisticsCollector collector;

	@BeforeEach
	void beforeEach() {
		collector = new DefaultCacheStatisticsCollector();
	}

	@Test // DATAREDIS-1082
	void collectsStatsPerCache() {

		collector.incGets(CACHE_1);
		collector.incPuts(CACHE_2);

		assertThat(collector.getCacheStatistics(CACHE_1).getGets()).isOne();
		assertThat(collector.getCacheStatistics(CACHE_1).getPuts()).isZero();

		assertThat(collector.getCacheStatistics(CACHE_2).getGets()).isZero();
		assertThat(collector.getCacheStatistics(CACHE_2).getPuts()).isOne();
	}

	@Test // DATAREDIS-1082
	void returnsEmptyStatsForCacheWithoutStatsSet() {

		assertThat(collector.getCacheStatistics(CACHE_1).getGets()).isZero();
		assertThat(collector.getCacheStatistics(CACHE_1).getPuts()).isZero();
		assertThat(collector.getCacheStatistics(CACHE_1).getDeletes()).isZero();
	}

	@Test // DATAREDIS-1082
	void returnsSnapshotOnGet() {

		collector.incGets(CACHE_1);

		CacheStatistics stats = collector.getCacheStatistics(CACHE_1);
		assertThat(stats.getGets()).isOne();

		collector.incGets(CACHE_1);

		assertThat(stats.getGets()).isOne();
		assertThat(collector.getCacheStatistics(CACHE_1).getGets()).isEqualTo(2);
	}

	@Test // DATAREDIS-1082
	void resetClearsData() {

		collector.incGets(CACHE_1);

		CacheStatistics stats = collector.getCacheStatistics(CACHE_1);
		assertThat(stats.getGets()).isOne();

		collector.reset(CACHE_1);

		assertThat(stats.getGets()).isOne();
		assertThat(collector.getCacheStatistics(CACHE_1).getGets()).isZero();
	}
}
