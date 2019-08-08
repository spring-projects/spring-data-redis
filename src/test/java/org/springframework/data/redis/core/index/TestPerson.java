package org.springframework.data.redis.core.index;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.index.Indexed;

@RedisHash("TestPerson")
public class TestPerson {

    @Id
    public UUID id;
    // Simple index defined in indexConfiguration
    public String firstName;
    @Indexed 
    public String lastName;
    // Composite sorting index sample. 
    // The index name is derived from state name in the address
    // while sorting by persont's created time.
    public TestAddress address;
    public TestGender gender;
    // SortingIndex defined in indexConfigration
    public int age;
    public List<TestPerson> children;
    // Sorting index defined in indexConfiguration
    public Date createdTimestamp = new Date();
    public Date updatedTimestamp = new Date();
}
