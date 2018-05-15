/*
 * Copyright 2015-2018 the original author or authors.
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
package org.springframework.data.redis.repository.query;

import java.util.Iterator;

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
import org.springframework.util.CollectionUtils;

/**
 * Redis specific query creator.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class RedisQueryCreator extends AbstractQueryCreator<KeyValueQuery<RedisOperationChain>, RedisOperationChain> {

	public RedisQueryCreator(PartTree tree, ParameterAccessor parameters) {
		super(tree, parameters);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#create(org.springframework.data.repository.query.parser.Part, java.util.Iterator)
	 */
	@Override
	protected RedisOperationChain create(Part part, Iterator<Object> iterator) {
		return from(part, iterator, new RedisOperationChain());
	}

	private RedisOperationChain from(Part part, Iterator<Object> iterator, RedisOperationChain sink) {

		switch (part.getType()) {
			case SIMPLE_PROPERTY:
				sink.sismember(part.getProperty().toDotPath(), iterator.next());
				break;
			case TRUE:
				sink.sismember(part.getProperty().toDotPath(), true);
				break;
			case FALSE:
				sink.sismember(part.getProperty().toDotPath(), false);
				break;
			case WITHIN:
			case NEAR:
				sink.near(getNearPath(part, iterator));
				break;
			default:
				throw new IllegalArgumentException(String.format("%s is not supported for Redis query derivation!", part.getType()));
		}

		return sink;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#and(org.springframework.data.repository.query.parser.Part, java.lang.Object, java.util.Iterator)
	 */
	@Override
	protected RedisOperationChain and(Part part, RedisOperationChain base, Iterator<Object> iterator) {
		return from(part, iterator, base);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#or(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected RedisOperationChain or(RedisOperationChain base, RedisOperationChain criteria) {
		base.orSismember(criteria.getSismember());
		return base;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#complete(java.lang.Object, org.springframework.data.domain.Sort)
	 */
	@Override
	protected KeyValueQuery<RedisOperationChain> complete(final RedisOperationChain criteria, Sort sort) {

		KeyValueQuery<RedisOperationChain> query = new KeyValueQuery<>(criteria);

		if (query.getCriteria() != null && !CollectionUtils.isEmpty(query.getCriteria().getSismember())
				&& !CollectionUtils.isEmpty(query.getCriteria().getOrSismember()))
			if (query.getCriteria().getSismember().size() == 1 && query.getCriteria().getOrSismember().size() == 1) {

				query.getCriteria().getOrSismember().add(query.getCriteria().getSismember().iterator().next());
				query.getCriteria().getSismember().clear();
			}

		if (sort.isSorted()) {
			query.setSort(sort);
		}

		return query;
	}

	private NearPath getNearPath(Part part, Iterator<Object> iterator) {

		Object o = iterator.next();

		Point point;
		Distance distance;

		if (o instanceof Circle) {

			point = ((Circle) o).getCenter();
			distance = ((Circle) o).getRadius();
		} else if (o instanceof Point) {

			point = (Point) o;

			if (!iterator.hasNext()) {
				throw new InvalidDataAccessApiUsageException(
						"Expected to find distance value for geo query. Are you missing a parameter?");
			}

			Object distObject = iterator.next();
			if (distObject instanceof Distance) {
				distance = (Distance) distObject;
			} else if (distObject instanceof Number) {
				distance = new Distance(((Number) distObject).doubleValue(), Metrics.KILOMETERS);
			} else {
				throw new InvalidDataAccessApiUsageException(String
						.format("Expected to find Distance or Numeric value for geo query but was %s.", distObject.getClass()));
			}
		} else {
			throw new InvalidDataAccessApiUsageException(
					String.format("Expected to find a Circle or Point/Distance for geo query but was %s.", o.getClass()));
		}

		return new NearPath(part.getProperty().toDotPath(), point, distance);
	}
}
