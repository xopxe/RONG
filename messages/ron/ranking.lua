local M = {}

local sched = require("lumen.sched")

local function mesage_quality(rong, m)
  local view = rong.view
  
	local q = 0
	--accumulated quality for a message
	for s,_ in pairs(m.matches) do
		if not view.own[s] then --don't accumulate quality from own subs
			q = q + s.meta.p 
		end
	end
	return q
end

local function find_worsts(rong)
  local inv = rong.inv
  local own = inv.own
	local worst_id, worst_q, worsts
	for mid, m in pairs(inv) do
		if not own[m] then
			local q = mesage_quality(rong, m)
			if not worst_q or q<worst_q then
				worst_id, worst_q = mid, q
				worsts = {}
			end
			if q == worst_q then
				worsts[#worsts+1]=mid --keep a list of entries p==min.
			end
			--if worst_p==0 then break end
		end
	end
	return worsts, worst_q
end

M.find_replaceable_homogeneous = function (rong) --FIXME
  local inv = rong.inv
  local conf = rong.conf
	local now = sched.get_time()

	if inv.own:len() < conf.reserved_owns then
		--guarantee for owns satisfied. find replacement between not owns

		local worsts, worst_q=find_worsts(messages)

		local number_of_ranges = conf.number_of_ranges
		local ranking_window = conf.ranking_window
		local range_count={}
		for i=1,number_of_ranges do range_count[i] = {} end
		--classify in ranges
		for _, mid in ipairs(worsts) do
			local m = messages[mid]
			local age=(now-m.meta.init_time) + m.message._in_transit --estimated emission time
			if age > ranking_window then return mid end
			local range = math.floor(number_of_ranges * (age / ranking_window))+1
			if range > number_of_ranges then range = number_of_ranges end 
			local rangelist = range_count[range]
			rangelist[#rangelist+1] = mid
		end
		--find longest range
		local longest_range, longest_range_i
		for i=1, number_of_ranges do 
			local count = #(range_count[i])
			if not longest_range_i or count > longest_range then
				longest_range_i, longest_range = i, count
			end		
		end
		--in longest range find most seen
		local max_seen, max_seen_mid
		for _, mid in ipairs(range_count[longest_range_i]) do
			local m = inv[mid]
			local seen = m.seen
			if not m.own and (not max_seen_mid or max_seen < seen) then
				max_seen_mid, max_seen = mid, seen
			end
		end
		
		return max_seen_mid
	else --messages.own:len() >= conf.reserved_owns
		--too much owns. find oldest registered own 
		local min_ts, min_ts_mid
		for mid, m in pairs(inv.own) do
			if not min_ts_mid or min_ts > m.meta.init_time then
				min_ts_mid, min_ts = mid, m.meta.init_time
			end
		end

		return min_ts_mid
	end	
end

M.find_replaceable_seen_rate = function (rong) --FIXME
  local inv = rong.inv
  local conf = rong.conf
	local now = sched.get_time()

	if inv.own:len() < conf.reserved_owns then
		--guarantee for owns satisfied. find replacement between not owns

		local worsts, worst_q=find_worsts(inv)

		--between the worst, find most seen
		local max_seenrate, max_seenrate_mid
		for _, mid in ipairs(worsts) do
			local m = inv[mid]
			local age = now - m.meta.init_time
			local seenrate = m.seen / age
			if not m.own and age > conf.min_time_for_averaging
			and (not max_seenrate_mid or max_seenrate < seenrate) then
				max_seenrate_mid, max_seenrate = mid, seenrate
			end
		end
		
		return max_seenrate_mid
	else --messages.own:len() >= conf.reserved_owns
		--too much owns. find oldest registered own 
		local min_ts, min_ts_mid
		for mid, m in pairs(messages.own) do
			if not min_ts_mid or min_ts > m.meta.init_time then
				min_ts_mid, min_ts = mid, m.meta.init_time
			end
		end

		return min_ts_mid
	end	
end

M.find_replaceable_seen = function (rong) --FIXME
  local inv = rong.inv
  local conf = rong.conf
	local now = sched.get_time()

	if inv.own:len() < conf.reserved_owns then
		--guarantee for owns satisfied. find replacement between not owns

		local worsts, worst_q=find_worsts(inv)

		--between the worst, find most seen
		local max_seen, max_seen_mid
		for _, mid in ipairs(worsts) do
			local m = inv[mid]
			local seen = m.seen
			if not m.own and (not max_seen_mid or max_seen < seen) then
				max_seen_mid, max_seen = mid, seen
			end
		end
		
		return max_seen_mid
	else --messages.own:len() >= conf.reserved_owns
		--too much owns. find oldest registered own 
		local min_ts, min_ts_mid
		for mid, m in pairs(inv.own) do
			if not min_ts_mid or min_ts > m.meta.init_time then
				min_ts_mid, min_ts = mid, m.meta.init_time
			end
		end

		return min_ts_mid
	end	
end

M.find_replaceable_diversity_array = function (rong) --FIXME
  local inv = rong.inv
  local conf = rong.conf
	if inv.own:len() < conf.reserved_owns then
		--guarantee for owns satisfied. find replacement between not owns

		local worsts, worst_q=find_worsts(inv)
		--conf.log('looking for a replacement', #worsts, worst_q)
		
		local diversity_array = {}

		--between the worst, find the oldest
		local min_ts, min_ts_mid
		for _, mid in ipairs(worsts) do
			local m = inv[mid]
			if m.discard_sample then
				diversity_array[#diversity_array + 1] = mid
			end
			--conf.log('$$$$', min_ts_mid, min_ts, m.meta.init_time, m.message._in_transit )
			local em=m.meta.init_time - m.message._in_transit --estimated emission time
			--conf.log('looking for a replacement ---- ', mid, em)
			--local em=-m.emited
			--local em=m.message.notification_id
			if not m.own 
			and (not min_ts_mid or min_ts > em) 
			and m.emited > conf.min_n_broadcasts 
			and not m.discard_sample then
				min_ts_mid, min_ts = mid, em
			end
		end
		
		if min_ts_mid and math.random() <= conf.diversity_survival_quotient then
			inv[min_ts_mid].discard_sample = true
			if #diversity_array > conf.max_size_diversity_array then				
				min_ts_mid = diversity_array[math.random(#diversity_array)]
				conf.log("Diversity array full. Replacing.")
			else
				min_ts_mid = nil
				conf.log("Populating diversity array.")
			end
		end
		
		return min_ts_mid
	else --messages.own:len() >= conf.reserved_owns
		--too much owns. find oldest registered own 
		local min_ts, min_ts_mid
		for mid, m in pairs(inv.own) do
			if not min_ts_mid or min_ts > m.meta.init_time then
				min_ts_mid, min_ts = mid, m.meta.init_time
			end
		end

		return min_ts_mid
	end

end

local myname_hash

-- 
local function string_hash (str)
	local temp_hash = 0
	for i = 1, #str do
		temp_hash = temp_hash + string.byte(str, i)
	end
	return temp_hash
end

local function aging_hash (mid)
	myname_hash = myname_hash or string_hash(conf.my_name)
	local temp_hash = myname_hash + string_hash(mid)

	return (math.fmod(temp_hash,100) * ((1 - conf.max_aging_slower) / 100) ) + conf.max_aging_slower
end


M.find_replaceable_variable_aging = function (rong) --FIXME
  local inv = rong.inv
  local conf = rong.conf
  
	if inv.own:len() < conf.reserved_owns then
		--guarantee for owns satisfied. find replacement between not owns

		local worsts, worst_q=find_worsts(inv)
		--conf.log('looking for a replacement', #worsts, worst_q)

		--between the worst, find the oldest
		local min_ts, min_ts_mid
		for _, mid in ipairs(worsts) do
			local m = inv[mid]

			if not m.aging_slower and not m.own then
				m.aging_slower = aging_hash(mid)
			end

			local em=(m.meta.init_time - m.message._in_transit * m.aging_slower) --estimated emission time

			if not m.own 
			and (not min_ts_mid or min_ts > em) 
			and m.emited > conf.min_n_broadcasts then
				min_ts_mid, min_ts = mid, em
			end
		end

		return min_ts_mid
	else --messages.own:len() >= conf.reserved_owns
		--too much owns. find oldest registered own 
		local min_ts, min_ts_mid
		for mid, m in pairs(messages.own) do
			if not min_ts_mid or min_ts > m.meta.init_time then
				min_ts_mid, min_ts = mid, m.meta.init_time
			end
		end

		return min_ts_mid
	end
end

M.find_replaceable_window = function (rong) --FIXME
  local inv = rong.inv
  local conf = rong.conf
	local now = sched.get_time()

	if inv.own:len() < conf.reserved_owns then
		--guarantee for owns satisfied. find replacement between not owns

		local worsts, worst_q=find_worsts(inv)
		local candidate_random = {}

		--between the worst, find the oldest
		local min_ts, min_ts_mid
		for _, mid in ipairs(worsts) do
			local m = inv[mid]
			local em=m.meta.init_time - m.message._in_transit --estimated emission time
			if not m.message.own then
				candidate_random[#candidate_random+1]=mid
			end
			if not m.own and (not min_ts_mid or min_ts > em) then
				min_ts_mid, min_ts = mid, em
			end
		end
		
		--if oldest still too young, select one at random between candidates
		if #candidate_random>0 
		and (not min_ts or min_ts > now-conf.period_of_random_survival) then
			local i=math.random(1, #candidate_random)
			min_ts_mid = candidate_random[i]
			worst_q = mesage_quality(inv[min_ts_mid])
		end

		return min_ts_mid
	else --messages.own:len() >= conf.reserved_owns
		--too much owns. find oldest registered own 
		local min_ts, min_ts_mid
		for mid, m in pairs(inv.own) do
			if not min_ts_mid or min_ts > m.meta.init_time then
				min_ts_mid, min_ts = mid, m.meta.init_time
			end
		end

		return min_ts_mid
	end	
end


function M.find_replaceable_fifo (rong)
  local inv = rong.inv
  local conf = rong.conf

	if inv.own:len() < conf.reserved_owns then
		--guarantee for owns satisfied. find replacement between not owns

		local worsts, worst_q=find_worsts(rong)
		--conf.log('looking for a replacement', #worsts, worst_q)

		--between the worst, find the oldest
		local min_ts, min_ts_mid
		for _, mid in ipairs(worsts) do
			local m = inv[mid]
      local meta = m.meta
			--conf.log('$$$$', min_ts_mid, min_ts, m.meta.init_time, m.message._in_transit )
	
      --local em=meta.init_time - meta.message._in_transit --estimated emission time
      local em = meta.init_time
      
			--conf.log('looking for a replacement ---- ', mid, em)
			--local em=m.message.notification_id
			if not inv.own[m]
			and (not min_ts_mid or min_ts > em) 
			and m.meta.emited > conf.min_n_broadcasts then
				min_ts_mid, min_ts = mid, em
			end
		end

		return min_ts_mid
	else --messages.own:len() >= conf.reserved_owns
		--too much owns. find oldest registered own 
		local min_ts, min_ts_mid
		for mid, m in pairs(inv.own) do
			if not min_ts_mid or min_ts > m.meta.init_time then
				min_ts_mid, min_ts = mid, m.meta.init_time
			end
		end

		return min_ts_mid
	end
end

return M