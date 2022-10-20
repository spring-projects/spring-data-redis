/*
 * Copyright 2022 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce.observability;

import java.net.SocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.redis.connection.lettuce.observability.RedisObservation.HighCardinalityCommandKeyNames;
import org.springframework.lang.Nullable;

import io.lettuce.core.protocol.CompleteableCommand;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.tracing.TraceContext;
import io.lettuce.core.tracing.TraceContextProvider;
import io.lettuce.core.tracing.Tracer;
import io.lettuce.core.tracing.Tracer.Span;
import io.lettuce.core.tracing.TracerProvider;
import io.lettuce.core.tracing.Tracing;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import reactor.core.publisher.Mono;

/**
 * {@link Tracing} adapter using Micrometer's {@link Observation}. This adapter integrates with Micrometer to propagate
 * observations into timers, distributed traces and any other registered handlers. Observations include a set of tags
 * capturing Redis runtime information.
 * <h3>Capturing full statements</h3> This adapter can capture full statements when enabling
 * {@code includeCommandArgsInSpanTags}. You should carefully consider the impact of this setting as all command
 * arguments will be captured in traces including these that may contain sensitive details.
 *
 * @author Mark Paluch
 * @since 3.0
 */
public class MicrometerTracingAdapter implements Tracing {

	private static final Log log = LogFactory.getLog(MicrometerTracingAdapter.class);

	private final ObservationRegistry observationRegistry;
	private final String serviceName;
	private final boolean includeCommandArgsInSpanTags;

	private final LettuceObservationConvention observationConvention;

	/**
	 * Create a new {@link MicrometerTracingAdapter} instance.
	 *
	 * @param observationRegistry must not be {@literal null}.
	 * @param serviceName service name to be used.
	 */
	public MicrometerTracingAdapter(ObservationRegistry observationRegistry, String serviceName) {
		this(observationRegistry, serviceName, false);
	}

	/**
	 * Create a new {@link MicrometerTracingAdapter} instance.
	 *
	 * @param observationRegistry must not be {@literal null}.
	 * @param serviceName service name to be used.
	 * @param includeCommandArgsInSpanTags whether to attach the full command into the trace. Use this flag with caution
	 *          as sensitive arguments will be captured in the observation spans and metric tags.
	 */
	public MicrometerTracingAdapter(ObservationRegistry observationRegistry, String serviceName,
			boolean includeCommandArgsInSpanTags) {

		this.observationRegistry = observationRegistry;
		this.serviceName = serviceName;
		this.observationConvention = new DefaultLettuceObservationConvention(includeCommandArgsInSpanTags);
		this.includeCommandArgsInSpanTags = includeCommandArgsInSpanTags;
	}

	@Override
	public TracerProvider getTracerProvider() {
		return () -> new MicrometerTracer(observationRegistry);
	}

	@Override
	public TraceContextProvider initialTraceContextProvider() {
		return new MicrometerTraceContextProvider(observationRegistry);
	}

	@Override
	public boolean isEnabled() {
		return true;
	}

	@Override
	public boolean includeCommandArgsInSpanTags() {
		return includeCommandArgsInSpanTags;
	}

	@Override
	public Endpoint createEndpoint(SocketAddress socketAddress) {
		return new SocketAddressEndpoint(socketAddress);
	}

	/**
	 * {@link Tracer} implementation based on Micrometer's {@link ObservationRegistry}.
	 */
	class MicrometerTracer extends Tracer {

		private final ObservationRegistry observationRegistry;

		public MicrometerTracer(ObservationRegistry observationRegistry) {
			this.observationRegistry = observationRegistry;
		}

		@Override
		public Tracer.Span nextSpan() {
			return this.postProcessSpan(createObservation());
		}

		@Override
		public Tracer.Span nextSpan(TraceContext traceContext) {

			if (traceContext instanceof MicrometerTraceContext micrometerTraceContext) {

				return micrometerTraceContext.observation == null ? nextSpan()
						: postProcessSpan(createObservation().parentObservation(micrometerTraceContext.observation()));
			}

			return nextSpan();
		}

		private Observation createObservation() {
			return RedisObservation.REDIS_COMMAND_OBSERVATION.observation(observationRegistry,
					() -> new LettuceObservationContext(serviceName));
		}

		private Tracer.Span postProcessSpan(Observation observation) {

			return observation != null && !observation.isNoop()
					? new MicrometerSpan(observation.observationConvention(observationConvention))
					: NoOpSpan.INSTANCE;
		}
	}

	/**
	 * No-op {@link Span} implemementation.
	 */
	static class NoOpSpan extends Tracer.Span {

		static final NoOpSpan INSTANCE = new NoOpSpan();

		public NoOpSpan() {}

		@Override
		public Tracer.Span start(RedisCommand<?, ?, ?> command) {
			return this;
		}

		@Override
		public Tracer.Span name(String name) {
			return this;
		}

		@Override
		public Tracer.Span annotate(String value) {
			return this;
		}

		@Override
		public Tracer.Span tag(String key, String value) {
			return this;
		}

		@Override
		public Tracer.Span error(Throwable throwable) {
			return this;
		}

		@Override
		public Tracer.Span remoteEndpoint(Tracing.Endpoint endpoint) {
			return this;
		}

		@Override
		public void finish() {}
	}

	/**
	 * Micrometer {@link Observation}-based {@link Span} implementation.
	 */
	static class MicrometerSpan extends Tracer.Span {

		private final Observation observation;

		private @Nullable RedisCommand<?, ?, ?> command;

		public MicrometerSpan(Observation observation) {
			this.observation = observation;
		}

		@Override
		public Span start(RedisCommand<?, ?, ?> command) {

			((LettuceObservationContext) observation.getContext()).setCommand(command);

			this.command = command;

			if (log.isDebugEnabled()) {
				log.debug(String.format("Starting Observation for Command %s", command));
			}

			if (command instanceof CompleteableCommand<?> completeableCommand) {

				completeableCommand.onComplete((o, throwable) -> {

					if (command.getOutput() != null) {

						String error = command.getOutput().getError();
						if (error != null) {
							observation.highCardinalityKeyValue(HighCardinalityCommandKeyNames.ERROR.withValue(error));
						} else if (throwable != null) {
							error(throwable);
						}
					}

					finish();
				});
			} else {
				throw new IllegalArgumentException("Command " + command
						+ " must implement CompleteableCommand to attach Span completion to command completion");
			}

			observation.start();
			return this;
		}

		@Override
		public Span name(String name) {
			return this;
		}

		@Override
		public Span annotate(String annotation) {
			return this;
		}

		@Override
		public Span tag(String key, String value) {
			observation.highCardinalityKeyValue(key, value);
			return this;
		}

		@Override
		public Span error(Throwable throwable) {

			if (log.isDebugEnabled()) {
				log.debug(String.format("Attaching error to Observation for Command %s", command));
			}

			observation.error(throwable);
			return this;
		}

		@Override
		public Span remoteEndpoint(Endpoint endpoint) {

			((LettuceObservationContext) observation.getContext()).setEndpoint(endpoint);
			return this;
		}

		@Override
		public void finish() {

			if (log.isDebugEnabled()) {
				log.debug(String.format("Stopping Observation for Command %s", command));
			}

			observation.stop();
		}
	}

	/**
	 * {@link TraceContextProvider} using {@link ObservationRegistry}.
	 */
	record MicrometerTraceContextProvider(ObservationRegistry registry) implements TraceContextProvider {

		@Override
		public TraceContext getTraceContext() {

			Observation observation = registry.getCurrentObservation();

			if (observation == null) {
				return null;
			}

			return new MicrometerTraceContext(observation);
		}

		@Override
		public Mono<TraceContext> getTraceContextLater() {

			return Mono.deferContextual(Mono::justOrEmpty).filter((it) -> {
				return it.hasKey(TraceContext.class) || it.hasKey(Observation.class)
						|| it.hasKey(ObservationThreadLocalAccessor.KEY);
			}).map((it) -> {

				if (it.hasKey(Observation.class)) {
					return new MicrometerTraceContext(it.get(Observation.class));
				}

				if (it.hasKey(TraceContext.class)) {
					return it.get(TraceContext.class);
				}

				return new MicrometerTraceContext(it.get(ObservationThreadLocalAccessor.KEY));
			});
		}
	}

	/**
	 * {@link TraceContext} implementation using {@link Observation}.
	 *
	 * @param observation
	 */
	record MicrometerTraceContext(Observation observation) implements TraceContext {

	}
}
