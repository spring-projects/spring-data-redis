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
package org.springframework.data.redis.core.convert;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;

import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link Jsr310Converters}.
 *
 * @author Mark Paluch
 */
class Jsr310ConvertersTest {

	@Test // GH-2677
	void shouldReportSupportedTemporalTypes() {

		assertThat(Jsr310Converters.supports(Object.class)).isFalse();
		assertThat(Jsr310Converters.supports(Date.class)).isFalse();

		assertThat(Jsr310Converters.supports(Instant.class)).isTrue();
		assertThat(Jsr310Converters.supports(ZoneId.class)).isTrue();
		assertThat(Jsr310Converters.supports(ZonedDateTime.class)).isTrue();

		assertThat(Jsr310Converters.supports(LocalDateTime.class)).isTrue();
		assertThat(Jsr310Converters.supports(LocalDate.class)).isTrue();
		assertThat(Jsr310Converters.supports(LocalTime.class)).isTrue();

		assertThat(Jsr310Converters.supports(Duration.class)).isTrue();
		assertThat(Jsr310Converters.supports(Period.class)).isTrue();

		assertThat(Jsr310Converters.supports(OffsetTime.class)).isTrue();
		assertThat(Jsr310Converters.supports(OffsetDateTime.class)).isTrue();
	}
}
