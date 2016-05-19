local M = {}

local sched = require("lumen.sched")

local function not_on_path(rong)
  local inv, conf = rong.inv, rong.conf
  local own = inv.own
  local nop = {}
	for mid, m in pairs(inv) do
		if not own[mid] then
      local meta = m.meta
			if not meta.path[conf.name] then
        nop[mid] = m
			end
		end
	end
	return nop
end

function M.find_fifo_not_on_path (rong)
  local inv = rong.inv
  local conf = rong.conf

	if inv.own:len() < conf.reserved_owns then
		--guarantee for owns satisfied. find replacement between not owns
    
		local nop = not_on_path(rong)
		--conf.log('looking for a replacement', #worsts, worst_q)
    
		--between the not-on-path, find the oldest
		local min_ts, min_ts_mid
		for mid, m in pairs(nop) do
      local meta = m.meta
      
      local em = meta.store_time --meta.init_time      
      
--print ('?', mid, inv.own[mid], min_ts_mid, min_ts, em, meta.emited )
      
			if not inv.own[mid]
			and (not min_ts_mid or min_ts > em) 
			and meta.emited >= conf.min_n_broadcasts then
				min_ts_mid, min_ts = mid, em
			end
		end
--print ('yyy not owns not on path', min_ts_mid)
    
    --is all on path, search trough all
    if not min_ts_mid then 
      for mid, m in pairs(inv) do
        local meta = m.meta
        
        local em = meta.store_time --meta.init_time      
--print ('?', mid, inv.own[mid], min_ts_mid, min_ts, em, meta.emited )
        
        if not inv.own[mid]
        and (not min_ts_mid or min_ts > em) 
        and meta.emited >= conf.min_n_broadcasts then
          min_ts_mid, min_ts = mid, em
        end
      end
--print ('yyy not owns all', min_ts_mid)
    end
    
		return min_ts_mid
	else --messages.own:len() >= conf.reserved_owns
		--too much owns. find oldest registered own 
		local min_ts, min_ts_mid
		for mid, m in pairs(inv.own) do
      local meta = m.meta
      --if not min_ts_mid or min_ts > meta.init_time then
			--	min_ts_mid, min_ts = mid, meta.init_time
			--end
      if not min_ts_mid or min_ts > meta.store_time then
				min_ts_mid, min_ts = mid, meta.store_time
			end
		end
--print ('yyy owns', min_ts_mid)
    
		return min_ts_mid
	end
end

return M