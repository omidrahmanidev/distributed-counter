local old = redis.call('HGET', KEYS[1], 'version')
local incoming = ARGV[2]
if not old or #incoming > #old or (#incoming == #old and incoming > old) then
	redis.call('HSET', KEYS[1], 'count', ARGV[1], 'version', incoming)
	return 1
end
return 0
