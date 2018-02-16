/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.redis.connection;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.springframework.data.redis.connection.RedisZSetCommands.Weights;

/**
 * Unit tests for {@link org.springframework.data.redis.connection.RedisZSetCommands.Weights}.
 *
 * @author Mark Paluch
 */
public class WeightsUnitTests {

	@Test // DATAREDIS-746
	public void shouldCreateWeights() {

		assertThat(Weights.of(1, 2, 3).toArray()).contains(1, 2, 3);
		assertThat(Weights.of(1, 2d, 3).toArray()).contains(1d, 2d, 3d);
	}

	@Test // DATAREDIS-746
	public void shouldRejectCreationWithNull() {

		assertThatThrownBy(() -> Weights.of((int[]) null)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> Weights.of((double[]) null)).isInstanceOf(IllegalArgumentException.class);
	}

	@Test // DATAREDIS-746
	public void shouldCreateEqualWeights() {

		Weights weights = Weights.fromSetCount(3);
		assertThat(weights.getWeight(0)).isOne();
		assertThat(weights.getWeight(1)).isOne();
		assertThat(weights.getWeight(2)).isOne();
	}

	@Test // DATAREDIS-746
	public void getShouldThrowIndexOutOfBoundsException() {

		assertThatThrownBy(() -> Weights.fromSetCount(1).getWeight(1)).isInstanceOf(IndexOutOfBoundsException.class);
		assertThatThrownBy(() -> Weights.fromSetCount(1).getWeight(-1)).isInstanceOf(IndexOutOfBoundsException.class);
	}

	@Test // DATAREDIS-746
	public void shouldMultiplyDouble() {

		Weights weights = Weights.of(1, 2, 3).multiply(2.5);
		assertThat(weights.getWeight(0)).isEqualTo(2.5);
		assertThat(weights.getWeight(2)).isEqualTo(7.5);
	}

	@Test // DATAREDIS-746
	public void shouldMultiplyInt() {

		Weights weights = Weights.of(1, 2, 3).multiply(2);
		assertThat(weights.getWeight(0)).isEqualTo(2);
		assertThat(weights.getWeight(2)).isEqualTo(6);
	}
}
