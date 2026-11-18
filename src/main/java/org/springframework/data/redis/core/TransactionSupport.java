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
package org.springframework.data.redis.core;

/**
 * Defines how {@link RedisTemplate} and {@link RedisConnectionUtils} participate in Spring-managed transactions.
 *
 * @author Moritz Halbritter
 * @since 4.3
 * @see RedisTemplate#setTransactionSupport(TransactionSupport)
 * @see org.springframework.transaction.support.TransactionSynchronizationManager#isActualTransactionActive()
 */
public enum TransactionSupport {

	/**
	 * Join an ongoing Redis transaction or start one ({@literal MULTI}) when a Spring-managed transaction is active.
	 * Write commands are queued and executed with {@literal EXEC} on commit or discarded with {@literal DISCARD} on
	 * rollback. Read-only commands, e.g. {@literal GET}, are executed immediately on a separate connection. Read-only
	 * transactions issue no {@literal MULTI}. Never creates a Spring-managed transaction.
	 */
	SYNCHRONIZED,

	/**
	 * Join an ongoing Redis transaction, for example one started by {@link #SYNCHRONIZED}, but never start one. Unlike
	 * {@link org.springframework.transaction.annotation.Propagation#SUPPORTS}, an active Spring-managed transaction
	 * without an ongoing Redis transaction executes commands non-transactionally.
	 */
	SUPPORTS,

	/**
	 * Never join the Redis transaction of a Spring-managed transaction, so commands are executed immediately and are
	 * not rolled back. Unlike {@link org.springframework.transaction.annotation.Propagation#NEVER}, an ongoing
	 * transaction does not cause an exception.
	 */
	DISABLED

}
