local M = {}

local log = require 'lumen.log'
local sched = require 'lumen.sched'

--selects messages matching a set of subscriptions
--parameter should be set of subscription ids.
M.select_matching = function (rong, vs)
  local inv, view, conf  = rong.inv, rong.view, rong.conf
  --local pending = rong.pending

  local ret = {}

	local now=sched.get_time()
	for mid, m in pairs(inv) do
		local matches=m.matches
		for sid, _ in pairs(vs) do
			local s=view[sid]
			if s and matches[s] then
				s.meta.last_success=now
        if now-m.meta.last_seen > (conf.delay_message_emit or 0) then
          --ret[mid] = m
          --pending:add(mid, m.data)
          ret[mid] = ret[mid] or {}
          ret[mid][sid] = s
				end
			end
		end
	end
  return ret
end

--whether a given message satisfies a filter
M.satisfies = function (m, filter)
	local is_match=true
  for i=1, #filter do
    local expr = filter[i]
		local ev_value, filt_op, filt_value = m[expr[1]], expr[2], expr[3]
    if (filt_op ~= '!=' and ev_value == nil)
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


return M
