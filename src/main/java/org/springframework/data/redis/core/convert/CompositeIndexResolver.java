/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.redis.core.convert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * Composite {@link IndexResolver} implementation that iterates over a given collection of delegate
 * {@link IndexResolver} instances. <br />
 * <br />
 * <strong>NOTE</strong> {@link IndexedData} created by an {@link IndexResolver} can be overwritten by subsequent
 * {@link IndexResolver}.
 *
 * @author Christoph Strobl
 * @since 1.7
 */
public class CompositeIndexResolver implements IndexResolver {

	private final List<IndexResolver> resolvers;

	/**
	 * Create new {@link CompositeIndexResolver}.
	 *
	 * @param resolvers must not be {@literal null}.
	 */
	public CompositeIndexResolver(Collection<IndexResolver> resolvers) {

		Assert.notNull(resolvers, "Resolvers must not be null!");
		if (CollectionUtils.contains(resolvers.iterator(), null)) {
			throw new IllegalArgumentException("Resolvers must no contain null values");
		}
		this.resolvers = new ArrayList<>(resolvers);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.IndexResolver#resolveIndexesFor(org.springframework.data.util.TypeInformation, java.lang.Object)
	 */
	@Override
	public Set<IndexedData> resolveIndexesFor(TypeInformation<?> typeInformation, @Nullable Object value) {

		if (resolvers.isEmpty()) {
			return Collections.emptySet();
		}

		Set<IndexedData> data = new LinkedHashSet<>();
		for (IndexResolver resolver : resolvers) {
			data.addAll(resolver.resolveIndexesFor(typeInformation, value));
		}
		return data;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.IndexResolver#resolveIndexesFor(java.lang.String, java.lang.String, org.springframework.data.util.TypeInformation, java.lang.Object)
	 */
	@Override
	public Set<IndexedData> resolveIndexesFor(String keyspace, String path, TypeInformation<?> typeInformation,
			@Nullable Object value) {

		Set<IndexedData> data = new LinkedHashSet<>();
		for (IndexResolver resolver : resolvers) {
			data.addAll(resolver.resolveIndexesFor(keyspace, path, typeInformation, value));
		}
		return data;
	}

}
