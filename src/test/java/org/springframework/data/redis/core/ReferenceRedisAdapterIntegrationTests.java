package org.springframework.data.redis.core;

import lombok.Data;
import lombok.experimental.Accessors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Reference;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.core.convert.*;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.hash.ObjectHashMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Unit tests for {@link ReferenceMappingRedisConverter}
 *
 * @author Ilya Viaznin
 */
@ExtendWith(LettuceConnectionFactoryExtension.class)
public class ReferenceRedisAdapterIntegrationTests {

    private final RedisConnectionFactory connectionFactory;

    public ReferenceRedisAdapterIntegrationTests(RedisConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    private ReferenceRedisAdapter adapter;

    private User user;

    private Employee employee;

    @BeforeEach
    void setUp() {
        var mappingContext = new RedisMappingContext(new MappingConfiguration(new IndexConfiguration(), new KeyspaceConfiguration()));
        var converter = new ReferenceMappingRedisConverter(mappingContext);
        var hashMapper = new ObjectHashMapper(converter);
        var template = new RedisTemplate<>(hashMapper);
        adapter = new ReferenceRedisAdapter(template, converter);

        converter.setIndexResolver(new PathIndexResolver(mappingContext));
        converter.setReferenceResolver(new ReferenceResolverImpl(template));
        template.setConnectionFactory(connectionFactory);

        adapter.afterPropertiesSet();
        converter.afterPropertiesSet();
        template.afterPropertiesSet();

        user = new User();
        employee = new Employee().setUser(user);
        user.setEmployee(employee);
    }

    @AfterEach
    void clean() {
        adapter.deleteAllOf(User.class.getName());
        adapter.deleteAllOf(Employee.class.getName());
    }

    @Test
    void getRecordWithCyclicReferenceNoStackOverflow() {
        user.setId(0L)
            .setName("Sam");
        employee.setId(0L);

        adapter.put(user.getId(), user, User.class.getName());
        adapter.put(employee.getId(), employee, Employee.class.getName());

        assertThatNoException().isThrownBy(() -> adapter.get(user.getId(), User.class.getName(), User.class));
        assertThatNoException().isThrownBy(() -> adapter.get(employee.getId(), Employee.class.getName(), Employee.class));
    }

    @Test
    void getCyclicReferenceValuesIsCorrect() {
        user.setId(0L)
            .setName("Elena");
        employee.setId(0L);

        adapter.put(user.getId(), user, User.class.getName());
        adapter.put(employee.getId(), employee, Employee.class.getName());

        var userFromRedis = adapter.get(user.getId(), User.class.getName(), User.class);
        var employeeFromRedis = adapter.get(employee.getId(), Employee.class.getName(), Employee.class);

        assertThat(userFromRedis).isNotNull();
        assertThat(employeeFromRedis).isNotNull();

        assertThat(userFromRedis.getId()).isEqualTo(user.getId());
        assertThat(userFromRedis.getName()).isEqualTo(user.getName());
        assertThat(userFromRedis.getEmployee()).isNotNull();
        assertThat(userFromRedis.getEmployee().getId()).isEqualTo(employee.getId());

        assertThat(employeeFromRedis.getId()).isEqualTo(employee.getId());
        assertThat(employeeFromRedis.getUser()).isNotNull();
        assertThat(employeeFromRedis.getUser().getId()).isEqualTo(user.getId());
        assertThat(employeeFromRedis.getUser().getName()).isEqualTo(user.getName());
    }

    @Data
    @Accessors(chain = true)
    @RedisHash
    static class User {

        @Id
        private Long id;

        private String name;

        @Reference
        private Employee employee;
    }

    @Data
    @Accessors(chain = true)
    @RedisHash
    static class Employee {

        @Id
        private Long id;

        @Reference
        private User user;
    }
}
