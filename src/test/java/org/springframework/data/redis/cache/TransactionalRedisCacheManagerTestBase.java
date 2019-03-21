/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.redis.cache;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.annotation.Transactional;

/**
 * The idea would have been to provide {@link Configuration} here and just set {@link Rollback} accordingly in extending
 * classes. Unfortunately this does not work when running build with gradle under java6. It was also not possible to add
 * {@link Configuration} here and import it using {@link ContextConfiguration#classes()}. <br />
 * <br />
 * Therefore {@link ContextConfiguration} had to be duplicated in
 * {@link TransactionalRedisCacheManagerWithCommitUnitTests} and
 * {@link TransactionalRedisCacheManagerWithRollbackUnitTests}.
 * 
 * @author Christoph Strobl
 */
public abstract class TransactionalRedisCacheManagerTestBase {

	static class FooService {

		private @Autowired BarRepository repo;

		@Transactional
		public String foo() {
			return "foo" + repo.bar();
		}
	}

	static class BarRepository {

		@Cacheable("bar")
		public String bar() {
			return "bar";
		}

	}

}
