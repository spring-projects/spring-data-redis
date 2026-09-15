/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.annotation;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.MapPropertySource;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.config.MethodRedisListenerEndpoint;
import org.springframework.data.redis.config.RedisListenerBootstrapConfiguration;
import org.springframework.data.redis.config.RedisListenerConfigUtils;
import org.springframework.data.redis.config.RedisListenerConfigurer;
import org.springframework.data.redis.config.RedisListenerEndpointRegistrar;
import org.springframework.data.redis.config.RedisListenerEndpointRegistry;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.StringMessage;
import org.springframework.data.redis.listener.Topic;

/**
 * Integration test for {@link EnableRedisListeners} and {@link RedisListener}
 *
 * @author Ilyass Bougati
 * @author Mark Paluch
 * @author Dongliang Xie
 */
class RedisListenerAnnotationBeanPostProcessorIntegrationTests {

	@Test // GH-1004
	void registersListenerWithDefaultContainer() {

		AtomicReference<RedisListenerEndpointRegistry> registryRef = new AtomicReference<>();

		doWithContext(context -> {
			RedisMessageListenerContainer container = container(context);

			verify(container).addMessageListener(any(), any(Topic.class));

			RedisListenerEndpointRegistry registry = context.getBean(RedisListenerEndpointRegistry.class);
			assertThat(registry.isRunning()).isTrue();
			registryRef.set(registry);
		}, DefaultConfig.class, SimpleService.class);

		assertThat(registryRef.get().isRunning()).isFalse();
	}

	@Test // GH-3393
	void resolvesListenerTopicPlaceholder() {

		try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
			context.getEnvironment().getPropertySources()
					.addFirst(new MapPropertySource("redis-listener-test", Map.of("app.my-channel", "my-channel")));
			context.register(DefaultConfig.class, PlaceholderTopicService.class);
			context.refresh();

			RedisMessageListenerContainer container = container(context);
			ArgumentCaptor<Topic> topicCaptor = ArgumentCaptor.forClass(Topic.class);

			verify(container).addMessageListener(any(), topicCaptor.capture());
			assertThat(topicCaptor.getValue().getTopic()).isEqualTo("my-channel");
		}
	}

	@Test // GH-3393
	void resolvesListenerConsumesPlaceholder() {

		try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
			context.getEnvironment().getPropertySources()
					.addFirst(new MapPropertySource("redis-listener-test",
							Map.of("app.my-content-type", "application/json")));
			context.register(DefaultConfig.class, ConsumesPlaceholderService.class);
			context.refresh();

			RedisMessageListenerContainer container = container(context);
			ArgumentCaptor<MessageListener> listenerCaptor = ArgumentCaptor.forClass(MessageListener.class);

			verify(container).addMessageListener(listenerCaptor.capture(), any(Topic.class));

			listenerCaptor.getValue().onMessage(
					new StringMessage("test-topic", "{\"firstname\":\"Walter\",\"lastname\":\"White\"}"), null);

			ConsumesPlaceholderService service = context.getBean(ConsumesPlaceholderService.class);
			assertThat(service.person.get()).isEqualTo(new Person("Walter", "White"));
		}
	}

	@Test // GH-3340
	void registersListenerWithNamedContainer() {

		doWithContext(context -> {
			RedisMessageListenerContainer customContainer = context.getBean("customContainer1",
					RedisMessageListenerContainer.class);
			RedisMessageListenerContainer defaultContainer = container(context);

			verify(customContainer).addMessageListener(any(), any(Topic.class));
			verify(defaultContainer, never()).addMessageListener(any(), any(Topic.class));
		}, DefaultConfig.class, MultiContainerService.class, CustomContainerConfig.class);
	}

	@Test // GH-3340
	void registersListenersAcrossMultipleContainers() {

		doWithContext(context -> {
			RedisMessageListenerContainer containerOne = context.getBean("customContainer1",
					RedisMessageListenerContainer.class);
			RedisMessageListenerContainer containerTwo = context.getBean("customContainer2",
					RedisMessageListenerContainer.class);

			verify(containerOne).addMessageListener(any(), any(Topic.class));
			verify(containerTwo).addMessageListener(any(), any(Topic.class));
		}, CustomContainerConfig.class, MultiContainerService.class);
	}

	@Test // GH-3340
	void failsWithMissingNamedContainer() {

		assertThatThrownBy(() -> new AnnotationConfigApplicationContext(DefaultConfig.class, NamedContainerService.class))
				.hasRootCauseInstanceOf(NoSuchBeanDefinitionException.class).hasMessageContaining("customContainer");
	}

	@Test // GH-3340
	void registersListenersMultipleContainers() {

		doWithContext(context -> {
			RedisMessageListenerContainer container = container(context);

			verify(container).addMessageListener(any(), any(Topic.class));
		}, DefaultConfig.class, CustomContainerConfig.class, UnnamedContainerService.class);
	}

	@Test // GH-3340
	void registrationFailsOnUnresolvableContainer() {

		assertThatExceptionOfType(BeanCreationException.class)
				.isThrownBy(() -> doWithContext(context -> {}, CustomContainerConfig.class, UnnamedContainerService.class));
	}

	@Test // GH-3439
	void deliversSubscriptionNotificationsWhenServiceImplementsSubscriptionListener() {

		doWithContext(context -> {
			RedisMessageListenerContainer container = container(context);

			ArgumentCaptor<MessageListener> listenerCaptor = ArgumentCaptor.forClass(MessageListener.class);
			verify(container, times(2)).addMessageListener(listenerCaptor.capture(), any(Topic.class));

			MessageListener listener = listenerCaptor.getAllValues().get(0);
			assertThat(listener).isInstanceOf(SubscriptionListener.class);

			((SubscriptionListener) listener).onChannelSubscribed("test-topic".getBytes(), 1);

			SubscriptionAwareService service = context.getBean(SubscriptionAwareService.class);
			assertThat(service.subscribedChannel.get()).isEqualTo("test-topic");
		}, DefaultConfig.class, SubscriptionAwareService.class);
	}

	@Test // GH-3439
	void forwardsSubscriptionCallbacksPerBeanWhenGrouped() {

		doWithContext(context -> {
			RedisMessageListenerContainer container = container(context);

			ArgumentCaptor<MessageListener> listenerCaptor = ArgumentCaptor.forClass(MessageListener.class);
			verify(container, times(2)).addMessageListener(listenerCaptor.capture(), eq(ChannelTopic.of("test-topic")));

			List<MessageListener> listeners = listenerCaptor.getAllValues();
			assertThat(listeners.get(0)).isNotSameAs(listeners.get(1));

			((SubscriptionListener) listeners.get(0)).onChannelSubscribed("test-topic".getBytes(), 1);

			SubscriptionAwareService serviceOne = context.getBean("serviceOne", SubscriptionAwareService.class);
			SubscriptionAwareService serviceTwo = context.getBean("serviceTwo", SubscriptionAwareService.class);

			// only the bean owning the notified listener saw the callback
			long notified = Stream.of(serviceOne, serviceTwo).filter(service -> service.subscribedChannel.get() != null)
					.count();
			assertThat(notified).isEqualTo(1);
		}, GroupingConfig.class, TwoBeansConfig.class);
	}

	@Test // GH-3439
	void keepsDefaultWhenBootstrapImportedDirectly() {

		doWithContext(context -> {
			RedisMessageListenerContainer container = container(context);

			verify(container, times(4)).addMessageListener(any(), any(Topic.class));
		}, BootstrapOnlyConfig.class, MultiChannelService.class);
	}

	@Test // GH-3439
	void doesNotGroupConfigurerEndpoints() {

		doWithContext(context -> {
			RedisMessageListenerContainer container = container(context);

			verify(container, times(2)).addMessageListener(any(), eq(ChannelTopic.of("configurer-channel")));
		}, GroupingConfig.class, ConfigurerConfig.class);
	}

	private static RedisMessageListenerContainer container(ApplicationContext context) {
		return context.getBean(RedisListenerConfigUtils.REDIS_MESSAGE_LISTENER_BEAN_NAME, RedisMessageListenerContainer.class);
	}

	private static void doWithContext(Consumer<ApplicationContext> action, Class<?>... annotatedClasses) {
		try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
			context.register(annotatedClasses);
			context.refresh();
			action.accept(context);
		}
	}

	@Configuration
	static class MockContainerConfig {

		@Bean
		public RedisMessageListenerContainer redisMessageListenerContainer() {
			return mock(RedisMessageListenerContainer.class);
		}
	}

	@Configuration
	@EnableRedisListeners
	@Import(MockContainerConfig.class)
	static class DefaultConfig {

	}

	@Configuration
	@EnableRedisListeners(grouping = ListenerGrouping.PER_BEAN_AND_TOPIC)
	@Import(MockContainerConfig.class)
	static class GroupingConfig {

	}

	@Configuration
	@Import({ RedisListenerBootstrapConfiguration.class, MockContainerConfig.class })
	static class BootstrapOnlyConfig {

	}

	@Configuration
	static class TwoBeansConfig {

		@Bean
		public SubscriptionAwareService serviceOne() {
			return new SubscriptionAwareService();
		}

		@Bean
		public SubscriptionAwareService serviceTwo() {
			return new SubscriptionAwareService();
		}

	}

	static class ConfigurerTargetService {

		@RedisListener(topic = "configurer-channel")
		public void annotated(String msg) {}

		public void manual(String msg) {}

	}

	@Configuration
	static class ConfigurerConfig {

		@Bean
		public ConfigurerTargetService configurerTargetService() {
			return new ConfigurerTargetService();
		}

		@Bean
		public RedisListenerConfigurer configurer(ConfigurerTargetService service, RedisMessageListenerContainer container) {

			return new RedisListenerConfigurer() {

				@Override
				public void configureRedisListeners(RedisListenerEndpointRegistrar registrar) {

					try {
						MethodRedisListenerEndpoint endpoint = new MethodRedisListenerEndpoint(service,
								ConfigurerTargetService.class.getMethod("manual", String.class));
						endpoint.setId("manual-endpoint");
						endpoint.setTopic("configurer-channel");
						registrar.registerEndpoint(endpoint, container);
					} catch (NoSuchMethodException ex) {
						throw new IllegalStateException(ex);
					}
				}
			};
		}

	}

	static class SimpleService {

		@RedisListener(topic = "test-topic")
		public void handle(String msg) {}

	}

	static class SubscriptionAwareService implements SubscriptionListener {

		final AtomicReference<String> subscribedChannel = new AtomicReference<>();

		@RedisListener(topic = "test-topic")
		public void a(String msg) {}

		@RedisListener(topic = "test-topic")
		public void b(String msg) {}

		@Override
		public void onChannelSubscribed(byte[] channel, long count) {
			this.subscribedChannel.set(new String(channel));
		}

	}

	static class MultiChannelService {

		@RedisListener(topic = "channel1")
		@RedisListener(topic = "channel2")
		public void a(String msg) {}

		@RedisListener(topic = "channel1")
		@RedisListener(topic = "channel2")
		public void b(String msg) {}

	}

	static class PlaceholderTopicService {

		@RedisListener(topic = "${app.my-channel}")
		public void handle(String msg) {}

	}

	static class ConsumesPlaceholderService {

		final AtomicReference<Person> person = new AtomicReference<>();

		@RedisListener(topic = "test-topic", consumes = "${app.my-content-type}")
		public void handle(Person person) {
			this.person.set(person);
		}

	}

	record Person(String firstname, String lastname) {

	}

	static class UnnamedContainerService {

		@RedisListener(topic = "test-topic", container = "")
		public void handle(String msg) {}

	}

	static class NamedContainerService {

		@RedisListener(container = "customContainer", topic = "test-topic")
		public void handle(String msg) {}

	}

	@Configuration
	@EnableRedisListeners
	static class CustomContainerConfig {

		@Bean
		public RedisMessageListenerContainer customContainer1() {
			return mock(RedisMessageListenerContainer.class);
		}

		@Bean
		public RedisMessageListenerContainer customContainer2() {
			return mock(RedisMessageListenerContainer.class);
		}

	}

	static class MultiContainerService {

		@RedisListener(container = "customContainer1", topic = "topic-one")
		@RedisListener(container = "customContainer2", topic = "topic-two")
		public void handle(String msg) {}

	}

}
