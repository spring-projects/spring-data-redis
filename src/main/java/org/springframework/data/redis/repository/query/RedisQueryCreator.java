/*
 * Copyright 2015-2025 the original author or authors.
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
package org.springframework.data.redis.repository.query;

import java.util.Collection;
import java.util.Iterator;

import org.jspecify.annotations.Nullable;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.redis.repository.query.RedisOperationChain.NearPath;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

/**
 * {@link AbstractQueryCreator} implementation for Redis.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author John Blum
 * @author Junghoon Ban
 * @since 1.7
 */
public class RedisQueryCreator extends AbstractQueryCreator<KeyValueQuery<RedisOperationChain>, RedisOperationChain> {

	public RedisQueryCreator(PartTree tree, ParameterAccessor parameters) {
		super(tree, parameters);
	}

	@Override
	protected RedisOperationChain create(Part part, Iterator<Object> iterator) {
		return from(part, iterator, new RedisOperationChain());
	}

	private RedisOperationChain from(Part part, Iterator<Object> iterator, RedisOperationChain sink) {

		switch (part.getType()) {
			case SIMPLE_PROPERTY -> sink.sismember(part.getProperty().toDotPath(), iterator.next());
			case TRUE -> sink.sismember(part.getProperty().toDotPath(), true);
			case FALSE -> sink.sismember(part.getProperty().toDotPath(), false);
			case WITHIN, NEAR -> sink.near(getNearPath(part, iterator));
			default ->
				throw new IllegalArgumentException("%s is not supported for Redis query derivation".formatted(part.getType()));
		}

		return sink;
	}

	@Override
	protected RedisOperationChain and(Part part, RedisOperationChain base, Iterator<Object> iterator) {
		return from(part, iterator, base);
	}

	@Override
	protected RedisOperationChain or(RedisOperationChain base, RedisOperationChain criteria) {
		base.orSismember(criteria.getSismember());
		return base;
	}

	@Override
	protected KeyValueQuery<RedisOperationChain> complete(@Nullable RedisOperationChain criteria, Sort sort) {

		KeyValueQuery<RedisOperationChain> query = new KeyValueQuery<>(criteria);

		if (criteria != null && containsExactlyOne(criteria.getSismember())
				&& containsExactlyOne(criteria.getOrSismember())) {
			criteria.getOrSismember().addAll(criteria.getSismember());
			criteria.getSismember().clear();
		}

		if (sort.isSorted()) {
			query.setSort(sort);
		}

		return query;
	}

	private NearPath getNearPath(Part part, Iterator<Object> iterator) {

		String path = part.getProperty().toDotPath();
		Object value = iterator.next();

		if (value instanceof Circle circle) {
			return new NearPath(path, circle.getCenter(), circle.getRadius());
		}

		if (value instanceof Point point) {

			if (!iterator.hasNext()) {
				throw new InvalidDataAccessApiUsageException(
						"Expected to find distance value for geo query;" + " Are you missing a parameter?");
			}

			Distance distance;
			Object distObject = iterator.next();

			if (distObject instanceof Distance dist) {
				distance = dist;
			} else if (distObject instanceof Number num) {
				distance = new Distance(num.doubleValue(), Metrics.KILOMETERS);
			} else {

				throw new InvalidDataAccessApiUsageException(
						"Expected to find Distance or Numeric value for geo query but was %s"
								.formatted(ClassUtils.getDescriptiveType(distObject)));
			}

			return new NearPath(path, point, distance);
		}

		throw new InvalidDataAccessApiUsageException("Expected to find a Circle or Point/Distance for geo query but was %s"
				.formatted(ClassUtils.getDescriptiveType(value.getClass())));
	}

	private static boolean containsExactlyOne(Collection<?> collection) {
		return !CollectionUtils.isEmpty(collection) && collection.size() == 1;
	}

}
