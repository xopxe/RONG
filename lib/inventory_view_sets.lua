local tracked_table=require 'rong.lib.tracked_table'

local weak_key = {__mode = 'k'}

local inv, view = {}, {}
local satisfies = require 'rong.lib.messaging'.satisfies
local log = require 'lumen.log'


local function make_MessageTable ()
	local function make_MessageTable_MT ()
		local n = 0
		local own_table = tracked_table.make_TrackedTable()
    getmetatable(own_table).__mode = 'kv'
    
		local MT={
			add=function(self, key, data, own)
				if not rawget(self, key) then
					n = n + 1
				end
        local entry = {
          id = key,
          meta = {},
          data = data,
        }
				rawset(self, key, entry)
				if own then own_table:add(key, entry) end
        
				--initialize matching cache table
				local matches={}
				setmetatable(matches, {__mode='k'})
				inv[key].matches=matches
				for sid, s in pairs(view) do
          matches[s] = satisfies(data, s.filter) --or nil
          log('RONG', 'DEBUG', 'Notification %s satisfies Subscription %s: %s',
            key, sid, tostring(matches[s]))
				end
        return entry
			end,
			del=function(self, key)
				if rawget(self, key) then
					n = n - 1
          own_table:del(key)
          rawset(self, key, nil)
				end
			end,
			len = function(self)
				return n
			end,
			own = own_table  
		}
		MT.__index=MT
		return MT
	end
	return setmetatable(inv,make_MessageTable_MT())
end

local function make_SubscriptionTable ()
	local function make_SubscriptionsTable_MT ()
		local n = 0
		local own_table = tracked_table.make_TrackedTable()
    getmetatable(own_table).__mode = 'kv'
    
		local MT={
			add=function(self, key, filter, own)
				if not rawget(self, key) then
					n = n + 1
				end
        local entry = {
          id = key,
          meta = {},
          filter = filter,
        }
				rawset(self, key, entry)
				if own then own_table:add(key, entry) end
        
				--update matching cache table in messages
				for mid,m in pairs(inv) do
          m.matches[entry] = satisfies(m.data, filter) --or nil
				end
        return entry
 			end,
      
      update=function(self, key, filter)
        local entry = assert(rawget(self, key))
        entry.filter=filter
				--update matching cache table in messages
				for mid,m in pairs(inv) do
          m.matches[entry] = satisfies(m.data, filter) --or nil
				end
        return entry
      end,
      
      del=function(self, key)
				if rawget(self, key) then
					n = n - 1
          rawset(self, key, nil)
          own_table:del(key)
				end
			end,
			len=function(self)
				return n
			end,
      own = own_table
		}
		MT.__index=MT
		return MT
	end
	return setmetatable(view,make_SubscriptionsTable_MT())
end


make_MessageTable()
make_SubscriptionTable()
--[[
return function()
  return messages, subscriptions
end
--]]
return {
  inv = inv, 
  view = view,
}


