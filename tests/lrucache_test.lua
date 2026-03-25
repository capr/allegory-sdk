require'lrucache'

--test basic put/get
local cache = lrucache{max_size = 3}
local k1, k2, k3, k4 = {}, {}, {}, {}
local v1, v2, v3, v4 = {}, {}, {}, {}
cache:put(k1, v1)
cache:put(k2, v2)
cache:put(k3, v3)
assert(cache:get(k1) == v1)
assert(cache:get(k2) == v2)
assert(cache:get(k3) == v3)
assert(cache:get(k4) == nil)

--test eviction: putting a 4th value evicts the least-recently used
cache:put(k4, v4)
assert(cache:get(k1) == nil) --evicted (LRU)
assert(cache:get(k4) == v4)

--test that get() refreshes LRU order
local cache = lrucache{max_size = 3}
cache:put(k1, v1)
cache:put(k2, v2)
cache:put(k3, v3)
cache:get(k1) --refresh k1, now k2 is LRU
cache:put(k4, v4) --should evict k2, not k1
assert(cache:get(k1) == v1)
assert(cache:get(k2) == nil) --evicted
assert(cache:get(k3) == v3)
assert(cache:get(k4) == v4)

--test replace same key
local cache = lrucache{max_size = 3}
cache:put(k1, v1)
cache:put(k2, v2)
cache:put(k1, v3) --replace k1's value
assert(cache:get(k1) == v3)
assert(cache:get(k2) == v2)

--test remove by key
local cache = lrucache{max_size = 3}
cache:put(k1, v1)
cache:put(k2, v2)
assert(cache:remove(k1) == v1)
assert(cache:get(k1) == nil)
assert(cache:remove(k3) == nil) --not in cache

--test remove_val
local cache = lrucache{max_size = 3}
cache:put(k1, v1)
cache:put(k2, v2)
assert(cache:remove_val(v1) == k1)
assert(cache:get(k1) == nil)
assert(cache:remove_val(v3) == nil) --not in cache

--test remove_last
local cache = lrucache{max_size = 3}
cache:put(k1, v1)
cache:put(k2, v2)
cache:put(k3, v3)
assert(cache:remove_last() == v1) --k1 is LRU
assert(cache:get(k1) == nil)

--test free_size
local cache = lrucache{max_size = 5}
cache:put(k1, v1)
cache:put(k2, v2)
assert(cache:free_size() == 3)

--test clear
local cache = lrucache{max_size = 3}
cache:put(k1, v1)
cache:put(k2, v2)
cache:clear()
assert(cache:get(k1) == nil)
assert(cache:get(k2) == nil)
assert(cache:free_size() == 3)

--test free
local cache = lrucache{max_size = 3}
cache:put(k1, v1)
cache:free()

--test custom value_size
local cache = lrucache{max_size = 10}
function cache:value_size(val) return val.size end
cache:put(k1, {size = 4})
cache:put(k2, {size = 4})
assert(cache:free_size() == 2)
cache:put(k3, {size = 3}) --should evict k1 to make room
assert(cache:get(k1) == nil)

--test free_value callback
local freed = {}
local cache = lrucache{max_size = 2}
function cache:free_value(val) freed[val] = true end
cache:put(k1, v1)
cache:put(k2, v2)
cache:put(k3, v3) --evicts v1
assert(freed[v1])

print'lrucache ok'
