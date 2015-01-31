local tracked_table=require 'lib.tracked_table'

local weak_key = {__mode = 'k'}

local inv, view = {}, {}
local inv_meta = setmetatable({}, weak_key)
local view_meta = setmetatable({}, weak_key)
local satisfies = require 'lib.messaging'.satisfies


local function make_MessageTable ()
	local function make_MessageTable_MT ()
		local n = 0
		local own_table = tracked_table.make_TrackedTable()
    getmetatable(own_table).__mode = 'kv'
    
		local MT={
			add=function(self, key, value, own)
				if not rawget(self, key) then
					n = n + 1
				end
				rawset(self, key, value)
        inv_meta[value] = {}
				if own then own_table:add(key, value) end
        
				--initialize matching cache table
				local matches={}
				setmetatable(matches, {__mode='k'})
				inv_meta[value].matches=matches
				for sid, s in pairs(view) do
					if satisfies(value, s) then
						--print ('M', sid, '+')
						matches[s] = true
					end 
				end
			end,
			del=function(self, key)
				if rawget(self, key) then
					n = n - 1
          view_meta[rawget(self, key)] = nil
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
			add=function(self, key, value, own)
				if not rawget(self, key) then
					n = n + 1
				end
				rawset(self, key, value)
        view_meta[value] = {}
				if own then own_table:add(key, value) end
        
				--update matching cache table in messages
				for mid,m in pairs(inv) do
					if satisfies(m, value) then
						--print ('S', mid, '+')
						inv_meta[m].matches[value]=true
					else
						--print ('S', mid, '-')
						inv_meta[m].matches[value]=nil
					end
				end
 			end,
	
  del=function(self, key)
				if rawget(self, key) then
					n = n - 1
          view_meta[rawget(self, key)] = nil
          own_table:del(key)
          rawset(self, key, nil)
				end
			end,
			len=function(self)
				return n
			end,
			own=function(self)
				return own_table
			end
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
  inv_meta = inv_meta, 
  view = view,
  view_meta = view_meta,
}


