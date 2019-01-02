/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.Range;
import io.lettuce.core.Range.Boundary;
import io.lettuce.core.codec.StringCodec;

import java.nio.ByteBuffer;
import java.util.function.Function;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class RangeConverter {

	static <T> Range<T> toRange(org.springframework.data.domain.Range<?> range) {
		return toRange(range, StringCodec.UTF8::encodeValue);
	}

	static <T> Range<T> toRange(org.springframework.data.domain.Range<?> range,
			Function<String, ? extends Object> stringEncoder) {
		return Range.from(lowerBoundArgOf(range, stringEncoder), upperBoundArgOf(range, stringEncoder));
	}

	@SuppressWarnings("unchecked")
	private static <T> Boundary<T> lowerBoundArgOf(org.springframework.data.domain.Range<?> range,
			Function<String, ? extends Object> stringEncoder) {
		return (Boundary<T>) rangeToBoundArgumentConverter(false, stringEncoder).convert(range);
	}

	@SuppressWarnings("unchecked")
	private static <T> Boundary<T> upperBoundArgOf(org.springframework.data.domain.Range<?> range,
			Function<String, ? extends Object> stringEncoder) {
		return (Boundary<T>) rangeToBoundArgumentConverter(true, stringEncoder).convert(range);
	}

	private static Converter<org.springframework.data.domain.Range<?>, Boundary<?>> rangeToBoundArgumentConverter(
			boolean upper, Function<String, ? extends Object> stringEncoder) {

		return (source) -> {

			Boolean inclusive = upper ? source.getUpperBound().isInclusive() : source.getLowerBound().isInclusive();
			Object value = upper ? source.getUpperBound().getValue().orElse(null)
					: source.getLowerBound().getValue().orElse(null);

			if (value instanceof Number) {
				return inclusive ? Boundary.including((Number) value) : Boundary.excluding((Number) value);
			}

			if (value instanceof String) {

				if (!StringUtils.hasText((String) value) || ObjectUtils.nullSafeEquals(value, "+")
						|| ObjectUtils.nullSafeEquals(value, "-")) {
					return Boundary.unbounded();
				}

				Object encoded = stringEncoder.apply((String) value);
				return inclusive ? Boundary.including(encoded) : Boundary.excluding(encoded);

			}

			if (value == null) {
				return Boundary.unbounded();
			}

			return inclusive ? Boundary.including((ByteBuffer) value) : Boundary.excluding((ByteBuffer) value);
		};
	}
}
