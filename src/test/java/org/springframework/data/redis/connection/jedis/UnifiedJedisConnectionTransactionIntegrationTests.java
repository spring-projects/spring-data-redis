/*
 * Copyright 2011-present the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration test of {@link JedisConnection} transaction functionality through
 * {@link redis.clients.jedis.UnifiedJedis}.
 *
 * @author Tihomir Mateev
 * @author Mark Paluch
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(value = "UnifiedJedisConnectionIntegrationTests-context.xml", inheritLocations = false)
public class UnifiedJedisConnectionTransactionIntegrationTests extends JedisConnectionTransactionIntegrationTests {

}
