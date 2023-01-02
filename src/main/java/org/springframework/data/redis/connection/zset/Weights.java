/*
 * Copyright 2022-2023 the original author or authors.
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
package org.springframework.data.redis.connection.zset;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Value object encapsulating a multiplication factor for each input sorted set. This means that the score of every
 * element in every input sorted set is multiplied by this factor before being passed to the aggregation function.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 */
public class Weights {

	private final List<Double> weights;

	private Weights(List<Double> weights) {
		this.weights = weights;
	}

	/**
	 * Create new {@link Weights} given {@code weights} as {@code int}.
	 *
	 * @param weights must not be {@literal null}.
	 * @return the {@link Weights} for {@code weights}.
	 */
	public static Weights of(int... weights) {

		Assert.notNull(weights, "Weights must not be null");
		return new Weights(Arrays.stream(weights).mapToDouble(value -> value).boxed().collect(Collectors.toList()));
	}

	/**
	 * Create new {@link Weights} given {@code weights} as {@code double}.
	 *
	 * @param weights must not be {@literal null}.
	 * @return the {@link Weights} for {@code weights}.
	 */
	public static Weights of(double... weights) {

		Assert.notNull(weights, "Weights must not be null");

		return new Weights(DoubleStream.of(weights).boxed().collect(Collectors.toList()));
	}

	/**
	 * Creates equal {@link Weights} for a number of input sets {@code count} with a weight of one.
	 *
	 * @param count number of input sets. Must be greater or equal to zero.
	 * @return equal {@link Weights} for a number of input sets with a weight of one.
	 */
	public static Weights fromSetCount(int count) {

		Assert.isTrue(count >= 0, "Count of input sorted sets must be greater or equal to zero");

		return new Weights(IntStream.range(0, count).mapToDouble(value -> 1).boxed().collect(Collectors.toList()));
	}

	/**
	 * Creates a new {@link Weights} object that contains all weights multiplied by {@code multiplier}
	 *
	 * @param multiplier multiplier used to multiply each weight with.
	 * @return equal {@link Weights} for a number of input sets with a weight of one.
	 */
	public Weights multiply(int multiplier) {
		return apply(it -> it * multiplier);
	}

	/**
	 * Creates a new {@link Weights} object that contains all weights multiplied by {@code multiplier}
	 *
	 * @param multiplier multiplier used to multiply each weight with.
	 * @return equal {@link Weights} for a number of input sets with a weight of one.
	 */
	public Weights multiply(double multiplier) {
		return apply(it -> it * multiplier);
	}

	/**
	 * Creates a new {@link Weights} object that contains all weights with {@link Function} applied.
	 *
	 * @param operator operator function.
	 * @return the new {@link Weights} with {@link DoubleUnaryOperator} applied.
	 */
	public Weights apply(Function<Double, Double> operator) {
		return new Weights(weights.stream().map(operator).collect(Collectors.toList()));
	}

	/**
	 * Retrieve the weight at {@code index}.
	 *
	 * @param index the weight index.
	 * @return the weight at {@code index}.
	 * @throws IndexOutOfBoundsException if the index is out of range
	 */
	public double getWeight(int index) {
		return weights.get(index);
	}

	/**
	 * @return number of weights.
	 */
	public int size() {
		return weights.size();
	}

	/**
	 * @return an array containing all of the weights in this list in proper sequence (from first to last element).
	 */
	public double[] toArray() {
		return weights.stream().mapToDouble(Double::doubleValue).toArray();
	}

	/**
	 * @return a {@link List} containing all of the weights in this list in proper sequence (from first to last element).
	 */
	public List<Double> toList() {
		return Collections.unmodifiableList(weights);
	}

	@Override
	public boolean equals(@Nullable Object o) {

		if (this == o) {
			return true;
		}

		if (!(o instanceof Weights)) {
			return false;
		}

		Weights that = (Weights) o;
		return ObjectUtils.nullSafeEquals(this.weights, that.weights);
	}

	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHashCode(weights);
	}
}
