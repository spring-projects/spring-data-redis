local size=redis.call('LLEN',KEYS[1])
local max = math.min(size,ARGV[1])
local t={}
for variable = 1, max, 1 do
  table.insert(t,redis.call('RPOP',KEYS[1]))
end
return t