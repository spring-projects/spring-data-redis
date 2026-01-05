/*
 * Copyright 2016-present the original author or authors.
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
package org.springframework.data.redis.core.types;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Expiration}.
 *
 * @author Mark Paluch
 * @author John Blum
 */
class ExpirationUnitTests {

	@Test // DATAREDIS-316
	void fromDefault() {

		Expiration expiration = Expiration.from(5, null);

		assertThat(expiration.getExpirationTime()).isEqualTo(5L);
		assertThat(expiration.getTimeUnit()).isEqualTo(TimeUnit.SECONDS);
	}

	@Test // DATAREDIS-316
	void fromNanos() {

		Expiration expiration = Expiration.from(5L * 1000 * 1000, TimeUnit.NANOSECONDS);

		assertThat(expiration.getExpirationTime()).isEqualTo(5L);
		assertThat(expiration.getTimeUnit()).isEqualTo(TimeUnit.MILLISECONDS);
	}

	@Test // DATAREDIS-316
	void fromMinutes() {

		Expiration expiration = Expiration.from(5, TimeUnit.MINUTES);

		assertThat(expiration.getExpirationTime()).isEqualTo(5L * 60);
		assertThat(expiration.getTimeUnit()).isEqualTo(TimeUnit.SECONDS);
	}

	@Test // GH-2351
	void equalValuedExpirationsAreEqual() {

		Expiration sixtyThousandMilliseconds = Expiration.milliseconds(60_000L);
		Expiration sixtySeconds = Expiration.seconds(60L);
		Expiration oneMinute = Expiration.from(1L, TimeUnit.MINUTES);

		assertThat(sixtyThousandMilliseconds).isEqualTo(sixtySeconds);
		assertThat(sixtySeconds).isEqualTo(oneMinute);
		assertThat(oneMinute).isEqualTo(sixtyThousandMilliseconds);
	}

	@Test // GH-2351
	void unequalValuedExpirationsAreNotEqual() {

		Expiration sixtySeconds = Expiration.seconds(60L);
		Expiration sixtyMilliseconds = Expiration.milliseconds(60L);

		assertThat(sixtySeconds).isNotEqualTo(sixtyMilliseconds);
	}

	@Test // GH-2351
	void hashCodeIsCorrect() {

		Expiration expiration = Expiration.seconds(60);

		assertThat(expiration).hasSameHashCodeAs(Expiration.seconds(60));
		assertThat(expiration).hasSameHashCodeAs(Expiration.from(Duration.ofSeconds(60L)));
		assertThat(expiration).hasSameHashCodeAs(Expiration.from(1, TimeUnit.MINUTES));
		assertThat(expiration).doesNotHaveSameHashCodeAs(60L);
		assertThat(expiration).doesNotHaveSameHashCodeAs(Duration.ofSeconds(60L));
		assertThat(expiration).doesNotHaveSameHashCodeAs(Expiration.from(60L, TimeUnit.MINUTES));
	}
}
