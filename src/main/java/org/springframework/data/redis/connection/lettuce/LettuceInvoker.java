/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Utility for functional invocation of {@link RedisClusterAsyncCommands Lettuce methods}. Typically used to express the
 * method call as method reference and passing method arguments through one of the {@code just} or {@code from} methods.
 * <p>
 * {@code just} methods record the method call and evaluate the method result immediately. {@code from} methods allows
 * composing a functional pipeline to transform the result using a {@link Converter}.
 * <p>
 * Usage example:
 *
 * <pre class="code">
 * LettuceInvoker invoker = …;
 *
 * Long result = invoker.just(RedisGeoAsyncCommands::geoadd, key, point.getX(), point.getY(), member);
 *
 * List&lt;byte[]&gt; result = invoker.fromMany(RedisGeoAsyncCommands::geohash, key, members)
 * 				.toList(it -> it.getValueOrElse(null));
 * </pre>
 * <p>
 * The actual translation from {@link RedisFuture} is delegated to {@link Synchronizer} which can either await
 * completion or record the future along {@link Converter} for further processing.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.5
 */
class LettuceInvoker {

	private final RedisClusterAsyncCommands<byte[], byte[]> connection;
	private final Synchronizer synchronizer;

	LettuceInvoker(RedisClusterAsyncCommands<byte[], byte[]> connection, Synchronizer synchronizer) {

		this.connection = connection;
		this.synchronizer = synchronizer;
	}

	/**
	 * Invoke the {@link ConnectionFunction0} and return its result.
	 *
	 * @param function must not be {@literal null}.
	 */
	@Nullable
	<R> R just(ConnectionFunction0<R> function) {

		Assert.notNull(function, "ConnectionFunction must not be null!");

		return synchronizer.invoke(() -> function.apply(connection), Converters.identityConverter(), () -> null);
	}

	/**
	 * Invoke the {@link ConnectionFunction1} and return its result.
	 *
	 * @param function must not be {@literal null}.
	 * @param t1 first argument.
	 */
	@Nullable
	<R, T1> R just(ConnectionFunction1<T1, R> function, T1 t1) {

		Assert.notNull(function, "ConnectionFunction must not be null!");

		return synchronizer.invoke(() -> function.apply(connection, t1));
	}

	/**
	 * Invoke the {@link ConnectionFunction2} and return its result.
	 *
	 * @param function must not be {@literal null}.
	 * @param t1 first argument.
	 * @param t2 second argument.
	 */
	@Nullable
	<R, T1, T2> R just(ConnectionFunction2<T1, T2, R> function, T1 t1, T2 t2) {

		Assert.notNull(function, "ConnectionFunction must not be null!");

		return synchronizer.invoke(() -> function.apply(connection, t1, t2));
	}

	/**
	 * Invoke the {@link ConnectionFunction3} and return its result.
	 *
	 * @param function must not be {@literal null}.
	 * @param t1 first argument.
	 * @param t2 second argument.
	 * @param t3 third argument.
	 */
	@Nullable
	<R, T1, T2, T3> R just(ConnectionFunction3<T1, T2, T3, R> function, T1 t1, T2 t2, T3 t3) {

		Assert.notNull(function, "ConnectionFunction must not be null!");

		return synchronizer.invoke(() -> function.apply(connection, t1, t2, t3));
	}

	/**
	 * Invoke the {@link ConnectionFunction4} and return its result.
	 *
	 * @param function must not be {@literal null}.
	 * @param t1 first argument.
	 * @param t2 second argument.
	 * @param t3 third argument.
	 * @param t4 fourth argument.
	 */
	@Nullable
	<R, T1, T2, T3, T4> R just(ConnectionFunction4<T1, T2, T3, T4, R> function, T1 t1, T2 t2, T3 t3, T4 t4) {

		Assert.notNull(function, "ConnectionFunction must not be null!");

		return synchronizer.invoke(() -> function.apply(connection, t1, t2, t3, t4));
	}

	/**
	 * Invoke the {@link ConnectionFunction5} and return its result.
	 *
	 * @param function must not be {@literal null}.
	 * @param t1 first argument.
	 * @param t2 second argument.
	 * @param t3 third argument.
	 * @param t4 fourth argument.
	 * @param t5 fifth argument.
	 */
	@Nullable
	<R, T1, T2, T3, T4, T5> R just(ConnectionFunction5<T1, T2, T3, T4, T5, R> function, T1 t1, T2 t2, T3 t3, T4 t4,
			T5 t5) {

		Assert.notNull(function, "ConnectionFunction must not be null!");

		return synchronizer.invoke(() -> function.apply(connection, t1, t2, t3, t4, t5));
	}

	/**
	 * Compose a invocation pipeline from the {@link ConnectionFunction0} and return a {@link SingleInvocationSpec} for
	 * further composition.
	 *
	 * @param function must not be {@literal null}.
	 */
	<R> SingleInvocationSpec<R> from(ConnectionFunction0<R> function) {

		Assert.notNull(function, "ConnectionFunction must not be null!");

		return new DefaultSingleInvocationSpec<>(() -> function.apply(connection), synchronizer);
	}

	/**
	 * Compose a invocation pipeline from the {@link ConnectionFunction1} and return a {@link SingleInvocationSpec} for
	 * further composition.
	 *
	 * @param function must not be {@literal null}.
	 * @param t1 first argument.
	 */
	<R, T1> SingleInvocationSpec<R> from(ConnectionFunction1<T1, R> function, T1 t1) {

		Assert.notNull(function, "ConnectionFunction must not be null!");

		return from(it -> function.apply(it, t1));
	}

	/**
	 * Compose a invocation pipeline from the {@link ConnectionFunction2} and return a {@link SingleInvocationSpec} for
	 * further composition.
	 *
	 * @param function must not be {@literal null}.
	 * @param t1 first argument.
	 * @param t2 second argument.
	 */
	<R, T1, T2> SingleInvocationSpec<R> from(ConnectionFunction2<T1, T2, R> function, T1 t1, T2 t2) {

		Assert.notNull(function, "ConnectionFunction must not be null!");

		return from(it -> function.apply(it, t1, t2));
	}

	/**
	 * Compose a invocation pipeline from the {@link ConnectionFunction3} and return a {@link SingleInvocationSpec} for
	 * further composition.
	 *
	 * @param function must not be {@literal null}.
	 * @param t1 first argument.
	 * @param t2 second argument.
	 * @param t3 third argument.
	 */
	<R, T1, T2, T3> SingleInvocationSpec<R> from(ConnectionFunction3<T1, T2, T3, R> function, T1 t1, T2 t2, T3 t3) {

		Assert.notNull(function, "ConnectionFunction must not be null!");

		return from(it -> function.apply(it, t1, t2, t3));
	}

	/**
	 * Compose a invocation pipeline from the {@link ConnectionFunction4} and return a {@link SingleInvocationSpec} for
	 * further composition.
	 *
	 * @param function must not be {@literal null}.
	 * @param t1 first argument.
	 * @param t2 second argument.
	 * @param t3 third argument.
	 * @param t4 fourth argument.
	 */
	<R, T1, T2, T3, T4> SingleInvocationSpec<R> from(ConnectionFunction4<T1, T2, T3, T4, R> function, T1 t1, T2 t2, T3 t3,
			T4 t4) {

		Assert.notNull(function, "ConnectionFunction must not be null!");

		return from(it -> function.apply(it, t1, t2, t3, t4));
	}

	/**
	 * Compose a invocation pipeline from the {@link ConnectionFunction5} and return a {@link SingleInvocationSpec} for
	 * further composition.
	 *
	 * @param function must not be {@literal null}.
	 * @param t1 first argument.
	 * @param t2 second argument.
	 * @param t3 third argument.
	 * @param t4 fourth argument.
	 * @param t5 fifth argument.
	 */
	<R, T1, T2, T3, T4, T5> SingleInvocationSpec<R> from(ConnectionFunction5<T1, T2, T3, T4, T5, R> function, T1 t1,
			T2 t2, T3 t3, T4 t4, T5 t5) {

		Assert.notNull(function, "ConnectionFunction must not be null!");

		return from(it -> function.apply(it, t1, t2, t3, t4, t5));
	}

	/**
	 * Compose a invocation pipeline from the {@link ConnectionFunction0} that returns a {@link Collection}-like result
	 * and return a {@link ManyInvocationSpec} for further composition.
	 *
	 * @param function must not be {@literal null}.
	 */
	<R extends Collection<E>, E> ManyInvocationSpec<E> fromMany(ConnectionFunction0<R> function) {

		Assert.notNull(function, "ConnectionFunction must not be null!");

		return new DefaultManyInvocationSpec<>(() -> function.apply(connection), synchronizer);
	}

	/**
	 * Compose a invocation pipeline from the {@link ConnectionFunction1} that returns a {@link Collection}-like result
	 * and return a {@link ManyInvocationSpec} for further composition.
	 *
	 * @param function must not be {@literal null}.
	 * @param t1 first argument.
	 */
	<R extends Collection<E>, E, T1> ManyInvocationSpec<E> fromMany(ConnectionFunction1<T1, R> function, T1 t1) {

		Assert.notNull(function, "ConnectionFunction must not be null!");

		return fromMany(it -> function.apply(it, t1));
	}

	/**
	 * Compose a invocation pipeline from the {@link ConnectionFunction2} that returns a {@link Collection}-like result
	 * and return a {@link ManyInvocationSpec} for further composition.
	 *
	 * @param function must not be {@literal null}.
	 * @param t1 first argument.
	 * @param t2 second argument.
	 */
	<R extends Collection<E>, E, T1, T2> ManyInvocationSpec<E> fromMany(ConnectionFunction2<T1, T2, R> function, T1 t1,
			T2 t2) {

		Assert.notNull(function, "ConnectionFunction must not be null!");

		return fromMany(it -> function.apply(it, t1, t2));
	}

	/**
	 * Compose a invocation pipeline from the {@link ConnectionFunction3} that returns a {@link Collection}-like result
	 * and return a {@link ManyInvocationSpec} for further composition.
	 *
	 * @param function must not be {@literal null}.
	 * @param t1 first argument.
	 * @param t2 second argument.
	 * @param t3 third argument.
	 */
	<R extends Collection<E>, E, T1, T2, T3> ManyInvocationSpec<E> fromMany(ConnectionFunction3<T1, T2, T3, R> function,
			T1 t1, T2 t2, T3 t3) {

		Assert.notNull(function, "ConnectionFunction must not be null!");

		return fromMany(it -> function.apply(it, t1, t2, t3));
	}

	/**
	 * Compose a invocation pipeline from the {@link ConnectionFunction4} that returns a {@link Collection}-like result
	 * and return a {@link ManyInvocationSpec} for further composition.
	 *
	 * @param function must not be {@literal null}.
	 * @param t1 first argument.
	 * @param t2 second argument.
	 * @param t3 third argument.
	 * @param t4 fourth argument.
	 */
	<R extends Collection<E>, E, T1, T2, T3, T4> ManyInvocationSpec<E> fromMany(
			ConnectionFunction4<T1, T2, T3, T4, R> function, T1 t1, T2 t2, T3 t3, T4 t4) {

		Assert.notNull(function, "ConnectionFunction must not be null!");

		return fromMany(it -> function.apply(it, t1, t2, t3, t4));
	}

	/**
	 * Compose a invocation pipeline from the {@link ConnectionFunction5} that returns a {@link Collection}-like result
	 * and return a {@link ManyInvocationSpec} for further composition.
	 *
	 * @param function must not be {@literal null}.
	 * @param t1 first argument.
	 * @param t2 second argument.
	 * @param t3 third argument.
	 * @param t4 fourth argument.
	 * @param t5 fifth argument.
	 */
	<R extends Collection<E>, E, T1, T2, T3, T4, T5> ManyInvocationSpec<E> fromMany(
			ConnectionFunction5<T1, T2, T3, T4, T5, R> function, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {

		Assert.notNull(function, "ConnectionFunction must not be null!");

		return fromMany(it -> function.apply(it, t1, t2, t3, t4, t5));
	}

	/**
	 * Represents an element in the invocation pipleline allowing consuming the result by applying a {@link Converter}.
	 *
	 * @param <S>
	 */
	interface SingleInvocationSpec<S> {

		/**
		 * Materialize the pipeline by invoking the {@code ConnectionFunction} and returning the result after applying
		 * {@link Converter}.
		 *
		 * @param converter must not be {@literal null}.
		 * @param <T> target type.
		 * @return the converted result, can be {@literal null}.
		 */
		@Nullable
		<T> T get(Converter<S, T> converter);

		/**
		 * Materialize the pipeline by invoking the {@code ConnectionFunction} and returning the result after applying
		 * {@link Converter} or return the {@literal nullDefault} value if not present.
		 *
		 * @param converter must not be {@literal null}.
		 * @param nullDefault can be {@literal null}.
		 * @param <T> target type.
		 * @return the converted result, can be {@literal null}.
		 */
		@Nullable
		default <T> T orElse(Converter<S, T> converter, @Nullable T nullDefault) {
			return getOrElse(converter, () -> nullDefault);
		}

		/**
		 * Materialize the pipeline by invoking the {@code ConnectionFunction} and returning the result after applying
		 * {@link Converter} or return the {@literal nullDefault} value if not present.
		 *
		 * @param converter must not be {@literal null}.
		 * @param nullDefault must not be {@literal null}.
		 * @param <T> target type.
		 * @return the converted result, can be {@literal null}.
		 */
		@Nullable
		<T> T getOrElse(Converter<S, T> converter, Supplier<T> nullDefault);
	}

	/**
	 * Represents an element in the invocation pipleline for methods returning {@link Collection}-like results allowing
	 * consuming the result by applying a {@link Converter}.
	 *
	 * @param <S>
	 */
	interface ManyInvocationSpec<S> {

		/**
		 * Materialize the pipeline by invoking the {@code ConnectionFunction} and returning the result.
		 *
		 * @return the result as {@link List}.
		 */
		default List<S> toList() {
			return toList(Converters.identityConverter());
		}

		/**
		 * Materialize the pipeline by invoking the {@code ConnectionFunction} and returning the result after applying
		 * {@link Converter}.
		 *
		 * @param converter must not be {@literal null}.
		 * @param <T> target type.
		 * @return the converted {@link List}.
		 */
		<T> List<T> toList(Converter<S, T> converter);

		/**
		 * Materialize the pipeline by invoking the {@code ConnectionFunction} and returning the result.
		 *
		 * @return the result as {@link Set}.
		 */
		default Set<S> toSet() {
			return toSet(Converters.identityConverter());
		}

		/**
		 * Materialize the pipeline by invoking the {@code ConnectionFunction} and returning the result after applying
		 * {@link Converter}.
		 *
		 * @param converter must not be {@literal null}.
		 * @param <T> target type.
		 * @return the converted {@link Set}.
		 */
		<T> Set<T> toSet(Converter<S, T> converter);
	}

	/**
	 * A function accepting {@link RedisClusterAsyncCommands} with 0 arguments.
	 *
	 * @param <R>
	 */
	@FunctionalInterface
	interface ConnectionFunction0<R> {

		/**
		 * Apply this function to the arguments and return a {@link RedisFuture}.
		 *
		 * @param connection the connection in use. Never {@literal null}.
		 */
		RedisFuture<R> apply(RedisClusterAsyncCommands<byte[], byte[]> connection);
	}

	/**
	 * A function accepting {@link RedisClusterAsyncCommands} with 1 argument.
	 *
	 * @param <T1>
	 * @param <R>
	 */
	@FunctionalInterface
	interface ConnectionFunction1<T1, R> {

		/**
		 * Apply this function to the arguments and return a {@link RedisFuture}.
		 *
		 * @param connection the connection in use. Never {@literal null}.
		 * @param t1 first argument.
		 */
		RedisFuture<R> apply(RedisClusterAsyncCommands<byte[], byte[]> connection, T1 t1);
	}

	/**
	 * A function accepting {@link RedisClusterAsyncCommands} with 2 arguments.
	 *
	 * @param <T1>
	 * @param <T2>
	 * @param <R>
	 */
	@FunctionalInterface
	interface ConnectionFunction2<T1, T2, R> {

		/**
		 * Apply this function to the arguments and return a {@link RedisFuture}.
		 *
		 * @param connection the connection in use. Never {@literal null}.
		 * @param t1 first argument.
		 * @param t2 second argument.
		 */
		RedisFuture<R> apply(RedisClusterAsyncCommands<byte[], byte[]> connection, T1 t1, T2 t2);
	}

	/**
	 * A function accepting {@link RedisClusterAsyncCommands} with 3 arguments.
	 *
	 * @param <T1>
	 * @param <T2>
	 * @param <T3>
	 * @param <R>
	 */
	@FunctionalInterface
	interface ConnectionFunction3<T1, T2, T3, R> {

		/**
		 * Apply this function to the arguments and return a {@link RedisFuture}.
		 *
		 * @param connection the connection in use. Never {@literal null}.
		 * @param t1 first argument.
		 * @param t2 second argument.
		 * @param t3 third argument.
		 */
		RedisFuture<R> apply(RedisClusterAsyncCommands<byte[], byte[]> connection, T1 t1, T2 t2, T3 t3);
	}

	/**
	 * A function accepting {@link RedisClusterAsyncCommands} with 4 arguments.
	 *
	 * @param <T1>
	 * @param <T2>
	 * @param <T3>
	 * @param <T4>
	 * @param <R>
	 */
	@FunctionalInterface
	interface ConnectionFunction4<T1, T2, T3, T4, R> {

		/**
		 * Apply this function to the arguments and return a {@link RedisFuture}.
		 *
		 * @param connection the connection in use. Never {@literal null}.
		 * @param t1 first argument.
		 * @param t2 second argument.
		 * @param t3 third argument.
		 * @param t4 fourth argument.
		 */
		RedisFuture<R> apply(RedisClusterAsyncCommands<byte[], byte[]> connection, T1 t1, T2 t2, T3 t3, T4 t4);
	}

	/**
	 * A function accepting {@link RedisClusterAsyncCommands} with 5 arguments.
	 *
	 * @param <T1>
	 * @param <T2>
	 * @param <T3>
	 * @param <T4>
	 * @param <T5>
	 * @param <R>
	 */
	@FunctionalInterface
	interface ConnectionFunction5<T1, T2, T3, T4, T5, R> {

		/**
		 * Apply this function to the arguments and return a {@link RedisFuture}.
		 *
		 * @param connection the connection in use. Never {@literal null}.
		 * @param t1 first argument.
		 * @param t2 second argument.
		 * @param t3 third argument.
		 * @param t4 fourth argument.
		 * @param t5 fifth argument.
		 */
		RedisFuture<R> apply(RedisClusterAsyncCommands<byte[], byte[]> connection, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5);
	}

	static class DefaultSingleInvocationSpec<S> implements SingleInvocationSpec<S> {

		private final Supplier<RedisFuture<S>> parent;
		private final Synchronizer synchronizer;

		public DefaultSingleInvocationSpec(Supplier<RedisFuture<S>> parent, Synchronizer synchronizer) {
			this.parent = parent;
			this.synchronizer = synchronizer;
		}

		@Override
		public <T> T get(Converter<S, T> converter) {

			Assert.notNull(converter, "Converter must not be null");

			return synchronizer.invoke(parent, converter, () -> null);
		}

		@Nullable
		@Override
		public <T> T getOrElse(Converter<S, T> converter, Supplier<T> nullDefault) {

			Assert.notNull(converter, "Converter must not be null!");

			return synchronizer.invoke(parent, converter, nullDefault);
		}
	}

	static class DefaultManyInvocationSpec<S> implements ManyInvocationSpec<S> {

		private final Supplier<RedisFuture<Collection<S>>> parent;
		private final Synchronizer synchronizer;

		public DefaultManyInvocationSpec(Supplier<RedisFuture<? extends Collection<S>>> parent, Synchronizer synchronizer) {

			this.parent = (Supplier) parent;
			this.synchronizer = synchronizer;
		}

		@Override
		public <T> List<T> toList(Converter<S, T> converter) {

			Assert.notNull(converter, "Converter must not be null!");

			return synchronizer.invoke(parent, source -> {

				if (source.isEmpty()) {
					return Collections.emptyList();
				}

				List<T> result = new ArrayList<>(source.size());

				for (S s : source) {
					result.add(converter.convert(s));
				}

				return result;
			}, Collections::emptyList);
		}

		@Override
		public <T> Set<T> toSet(Converter<S, T> converter) {

			Assert.notNull(converter, "Converter must not be null!");

			return synchronizer.invoke(parent, source -> {

				if (source.isEmpty()) {
					return Collections.emptySet();
				}

				Set<T> result = new LinkedHashSet<>(source.size());

				for (S s : source) {
					result.add(converter.convert(s));
				}

				return result;
			}, Collections::emptySet);
		}
	}

	/**
	 * Interface to define a synchronization function to evaluate {@link RedisFuture}.
	 */
	@FunctionalInterface
	interface Synchronizer {

		@Nullable
		@SuppressWarnings({ "unchecked", "rawtypes" })
		default <I, T> T invoke(Supplier<RedisFuture<I>> futureSupplier) {
			return (T) doInvoke((Supplier) futureSupplier, Converters.identityConverter(), () -> null);
		}

		@Nullable
		@SuppressWarnings({ "unchecked", "rawtypes" })
		default <I, T> T invoke(Supplier<RedisFuture<I>> futureSupplier, Converter<I, T> converter,
				Supplier<T> nullDefault) {

			return (T) doInvoke((Supplier) futureSupplier, (Converter<Object, Object>) converter,
					(Supplier<Object>) nullDefault);
		}

		@Nullable
		Object doInvoke(Supplier<RedisFuture<Object>> futureSupplier, Converter<Object, Object> converter,
				Supplier<Object> nullDefault);
	}
}
