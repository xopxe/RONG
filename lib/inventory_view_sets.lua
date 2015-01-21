local tracked_table=require 'lib.tracked_table'

local messages, view, view_meta = {}, {}, {}

--whether a given message satisfies a filter
local function satisfies(m, filter)
	local is_match=true
  for i=1, #filter do
    local expr = filter[i]
		local ev_value, filt_op, filt_value = m[expr[1]], expr[2], expr[3]
    if ev_value == nil
    or (filt_op == '=' and (ev_value~=filt_value))
    or (filt_op == '!=' and (ev_value==filt_value))
    or (filt_op == '>' and (ev_value<=filt_value))
    or (filt_op == '<' and (ev_value>=filt_value)) then
      is_match=false
      break
    end
	end
	return is_match
end


local function make_MessageTable ()
	local function make_MessageTable_MT ()
		local n = 0
		local own_table = tracked_table.make_TrackedTable()

		local MT={
			add=function(self, key, value)
				if not rawget(self, key) then
					n = n + 1
				end
				rawset(self, key, value)
				if value.own then own_table:add(key, value) end

				--initialize matching cache table
				local matches={}
				setmetatable(matches, {__mode='k'})
				value.matches=matches
				for sid, s in pairs(view) do
					if satisfies(value.message, s.filter) then
						--print ('M', sid, '+')
						matches[s] = true
					end 
				end
			end,
			del=function(self, key)
				if rawget(self, key) and n > 0 then
					n = n - 1
				end
				rawset(self, key, nil)
				own_table:del(key)
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
	return setmetatable(messages,make_MessageTable_MT())
end

local function make_SubscriptionTable ()
	local function make_SubscriptionsTable_MT ()
		local n = 0
		local own_table = tracked_table.make_TrackedTable()
    
		local MT={
			add=function(self, key, value, own)
				if not rawget(self, key) then
					n = n + 1
				end
				rawset(self, key, value)
        view_meta[key] = {}
				if own then own_table:add(key, value) end
        
				--update matching cache table in messages
				for mid,m in pairs(messages) do
					if satisfies(m.message, value.filter) then
						--print ('S', mid, '+')
						m.matches[value]=true
					else
						--print ('S', mid, '-')
						m.matches[value]=nil
					end
				end
        
        value.seq = value.seq or 0
        value.visited = value.visited or {} --{conf.name}
			end,
			del=function(self, key)
				if rawget(self, key) and n > 0 then
					n = n - 1
				end
				rawset(self, key, nil)
        view_meta[key] = nil
				own_table:del(key)
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
  messages = messages, 
  view = view,
  view_meta = view_meta,
}


