/*
 * Copyright 2024 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.DtoInstantiatingConverter;
import org.springframework.data.keyvalue.core.KeyValueOperations;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.keyvalue.repository.query.KeyValuePartTreeQuery;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.redis.core.RedisKeyValueAdapter;
import org.springframework.data.redis.core.convert.RedisConverter;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.data.util.Streamable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Redis-specific implementation of {@link KeyValuePartTreeQuery} supporting projections.
 *
 * @author Mark Paluch
 * @since 3.2.4
 */
public class RedisPartTreeQuery extends KeyValuePartTreeQuery {

	private final RedisKeyValueAdapter adapter;

	public RedisPartTreeQuery(QueryMethod queryMethod, QueryMethodEvaluationContextProvider evaluationContextProvider,
			KeyValueOperations template, Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
		super(queryMethod, evaluationContextProvider, template, queryCreator);
		this.adapter = (RedisKeyValueAdapter) template.getKeyValueAdapter();
	}

	@Override
	public Object execute(Object[] parameters) {

		ParameterAccessor accessor = new ParametersParameterAccessor(getQueryMethod().getParameters(), parameters);
		KeyValueQuery<?> query = prepareQuery(parameters);
		ResultProcessor processor = getQueryMethod().getResultProcessor().withDynamicProjection(accessor);

		RedisConverter converter = adapter.getConverter();
		Converter<Object, Object> resultPostProcessor = new ResultProcessingConverter(processor,
				converter.getMappingContext(), converter.getEntityInstantiators());

		Object source = doExecute(parameters, query);
		return source != null ? processor.processResult(resultPostProcessor.convert(source)) : null;
	}

	/**
	 * A {@link Converter} to post-process all source objects using the given {@link ResultProcessor}.
	 *
	 * @author Mark Paluch
	 */
	static final class ResultProcessingConverter implements Converter<Object, Object> {

		private final ResultProcessor processor;
		private final MappingContext<? extends PersistentEntity<?, ?>, ? extends PersistentProperty<?>> context;
		private final EntityInstantiators instantiators;

		public ResultProcessingConverter(ResultProcessor processor,
				MappingContext<? extends PersistentEntity<?, ?>, ? extends PersistentProperty<?>> context,
				EntityInstantiators instantiators) {

			Assert.notNull(processor, "Processor must not be null!");
			Assert.notNull(context, "MappingContext must not be null!");
			Assert.notNull(instantiators, "Instantiators must not be null!");

			this.processor = processor;
			this.context = context;
			this.instantiators = instantiators;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Object convert(Object source) {

			if (source instanceof Set<?> s) {

				Set<Object> target = new LinkedHashSet<>(s.size());

				for (Object o : s) {
					target.add(convert(o));
				}

				return target;
			}

			if (source instanceof Collection<?> c) {

				List<Object> target = new ArrayList<>(c.size());

				for (Object o : c) {
					target.add(convert(o));
				}

				return target;
			}

			if (source instanceof Streamable<?> s) {
				return s.map(this::convert);
			}

			ReturnedType returnedType = processor.getReturnedType();

			if (ReflectionUtils.isVoid(returnedType.getReturnedType())) {
				return null;
			}

			if (ClassUtils.isPrimitiveOrWrapper(returnedType.getReturnedType())) {
				return source;
			}

			Converter<Object, Object> converter = new DtoInstantiatingConverter(returnedType.getReturnedType(), context,
					instantiators);

			return processor.processResult(source, converter);
		}
	}
}
