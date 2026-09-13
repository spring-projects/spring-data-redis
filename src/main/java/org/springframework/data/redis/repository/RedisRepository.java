package org.springframework.data.redis.repository;

import org.springframework.data.keyvalue.repository.KeyValueRepository;

/**
 * Redis specific {@link org.springframework.data.repository.Repository} interface.
 *
 * @author Junghoon Ban
 * @param <T>
 * @param <ID>
 */
public interface RedisRepository<T, ID> extends KeyValueRepository<T, ID> {}
