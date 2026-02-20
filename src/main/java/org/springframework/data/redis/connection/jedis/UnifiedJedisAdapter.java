package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.AbstractTransaction;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.UnifiedJedis;

/**
 * Adapter that wraps a {@link Jedis} instance to provide the {@link UnifiedJedis} API.
 * Uses the {@link UnifiedJedis#UnifiedJedis(redis.clients.jedis.Connection)} constructor
 * which employs {@code SimpleCommandExecutor} that executes commands directly on the connection
 * without closing it after each command (unlike {@code DefaultCommandExecutor}).
 */
public class UnifiedJedisAdapter extends UnifiedJedis {

    private final Jedis jedis;

    public UnifiedJedisAdapter(Jedis jedis) {
        // Use the Connection-based constructor which uses SimpleCommandExecutor
        // This executor does NOT close the connection after each command
        super(jedis.getConnection());
        this.jedis = jedis;
    }

    public Jedis toJedis() {
        return jedis;
    }

    @Override
    public AbstractTransaction multi() {
        // Use Jedis-based Transaction which doesn't close the connection on Transaction.close()
        return new Transaction(jedis);
    }

    @Override
    public AbstractTransaction transaction(boolean doMulti) {
        // Use Jedis-based Transaction which doesn't close the connection on Transaction.close()
        return new Transaction(jedis.getConnection(), doMulti, false);
    }

    @Override
    public Pipeline pipelined() {
        // Use Jedis-based Pipeline which doesn't close the connection on Pipeline.close()
        return new Pipeline(jedis.getConnection(), false);
    }

    // PubSub methods - must override because parent uses provider.getConnection() which is null
    @Override
    public void subscribe(JedisPubSub jedisPubSub, String... channels) {
        jedisPubSub.proceed(jedis.getConnection(), channels);
    }

    @Override
    public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
        jedisPubSub.proceedWithPatterns(jedis.getConnection(), patterns);
    }

    @Override
    public void subscribe(BinaryJedisPubSub jedisPubSub, byte[]... channels) {
        jedisPubSub.proceed(jedis.getConnection(), channels);
    }

    @Override
    public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
        jedisPubSub.proceedWithPatterns(jedis.getConnection(), patterns);
    }
}
