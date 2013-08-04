local t={}
table.insert(t,redis.call('RPOP',KEYS[1]))
table.insert(t,redis.call('LLEN',KEYS[1]))
return t