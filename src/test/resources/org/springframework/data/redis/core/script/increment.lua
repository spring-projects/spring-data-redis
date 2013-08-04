if redis.call("EXISTS",KEYS[1]) == 1 then
  return redis.call("INCR",KEYS[1])
else
  return nil
end