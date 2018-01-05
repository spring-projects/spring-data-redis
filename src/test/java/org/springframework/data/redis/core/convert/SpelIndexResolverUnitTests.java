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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration.KeyspaceSettings;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.redis.core.index.SpelIndexDefinition;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.core.mapping.RedisPersistentEntity;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.expression.spel.SpelEvaluationException;

/**
 * @author Rob Winch
 * @author Christoph Strobl
 */
public class SpelIndexResolverUnitTests {

	String keyspace;

	String indexName;

	String username;

	SpelIndexResolver resolver;

	Session session;

	ClassTypeInformation<?> typeInformation;

	String securityContextAttrName;

	RedisMappingContext mappingContext;

	RedisPersistentEntity<?> entity;

	@Before
	public void setup() {

		username = "rob";
		keyspace = "spring:session:sessions";
		indexName = "principalName";
		securityContextAttrName = "SPRING_SECURITY_CONTEXT";

		typeInformation = ClassTypeInformation.from(Session.class);
		session = createSession();

		resolver = createWithExpression("getAttribute('" + securityContextAttrName + "')?.authentication?.name");
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-425
	public void constructorNullRedisMappingContext() {

		mappingContext = null;
		new SpelIndexResolver(mappingContext);
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-425
	public void constructorNullSpelExpressionParser() {
		new SpelIndexResolver(mappingContext, null);
	}

	@Test // DATAREDIS-425
	public void nullValue() {

		Set<IndexedData> indexes = resolver.resolveIndexesFor(typeInformation, null);

		assertThat(indexes.size(), equalTo(0));
	}

	@Test // DATAREDIS-425
	public void wrongKeyspace() {

		typeInformation = ClassTypeInformation.from(String.class);
		Set<IndexedData> indexes = resolver.resolveIndexesFor(typeInformation, "");

		assertThat(indexes.size(), equalTo(0));
	}

	@Test // DATAREDIS-425
	public void sessionAttributeNull() {

		session = new Session();
		Set<IndexedData> indexes = resolver.resolveIndexesFor(typeInformation, session);

		assertThat(indexes.size(), equalTo(0));
	}

	@Test // DATAREDIS-425
	public void resolvePrincipalName() {

		Set<IndexedData> indexes = resolver.resolveIndexesFor(typeInformation, session);

		assertThat(indexes, hasItem(new SimpleIndexedPropertyValue(keyspace, indexName, username)));
	}

	@Test(expected = SpelEvaluationException.class) // DATAREDIS-425
	public void spelError() {

		session.setAttribute(securityContextAttrName, "");

		resolver.resolveIndexesFor(typeInformation, session);
	}

	@Test // DATAREDIS-425
	public void withBeanAndThis() {

		this.resolver = createWithExpression("@bean.run(#this)");
		this.resolver.setBeanResolver((context, beanName) -> new Object() {
			@SuppressWarnings("unused")
			public Object run(Object arg) {
				return arg;
			}
		});

		Set<IndexedData> indexes = resolver.resolveIndexesFor(typeInformation, session);

		assertThat(indexes, hasItem(new SimpleIndexedPropertyValue(keyspace, indexName, session)));
	}

	private SpelIndexResolver createWithExpression(String expression) {

		SpelIndexDefinition principalIndex = new SpelIndexDefinition(keyspace, expression, indexName);
		IndexConfiguration configuration = new IndexConfiguration();
		configuration.addIndexDefinition(principalIndex);

		KeyspaceSettings keyspaceSettings = new KeyspaceSettings(Session.class, keyspace);
		KeyspaceConfiguration keyspaceConfiguration = new KeyspaceConfiguration();
		keyspaceConfiguration.addKeyspaceSettings(keyspaceSettings);

		MappingConfiguration mapping = new MappingConfiguration(configuration, keyspaceConfiguration);

		mappingContext = new RedisMappingContext(mapping);

		return new SpelIndexResolver(mappingContext);
	}

	private Session createSession() {

		Session session = new Session();
		session.setAttribute(securityContextAttrName, new SecurityContextImpl(new Authentication(username)));
		return session;
	}

	static class Session {

		private Map<String, Object> sessionAttrs = new HashMap<>();

		public void setAttribute(String attrName, Object attrValue) {
			this.sessionAttrs.put(attrName, attrValue);
		}

		public Object getAttribute(String attributeName) {
			return sessionAttrs.get(attributeName);
		}
	}

	static class SecurityContextImpl {
		private final Authentication authentication;

		public SecurityContextImpl(Authentication authentication) {
			this.authentication = authentication;
		}

		public Authentication getAuthentication() {
			return authentication;
		}
	}

	public static class Authentication {
		private final String principalName;

		public Authentication(String principalName) {
			this.principalName = principalName;
		}

		public String getName() {
			return principalName;
		}
	}
}
