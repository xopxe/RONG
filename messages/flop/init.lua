-- ron protocol

local M = {}

local log = require 'lumen.log'
local sched = require 'lumen.sched'
local messaging = require 'lib.messaging'
local encoder_lib = require 'lumen.lib.dkjson' --'lumen.lib.bencode'
local encode_f, decode_f = encoder_lib.encode, encoder_lib.decode

local view_merge = function(rong, vi)
  local now = sched.get_time()
  
  local view = rong.view
  local conf = rong.conf
    
  for sid, si in pairs(vi) do
    log('FLOP', 'DEBUG', 'Merging subscription: %s', tostring(sid))
    local sl = view[sid]
    if sl then
      local meta = sl.meta
      if meta.seq < si.seq then
        meta.seq = si.seq
        --FIXME??
        meta.visited = si.visited
        meta.visited[rong.conf.name] = true
        --/FIXME??
      end  
    else
      view:add(sid, si.filter, false)
      sl = view[sid]
      local meta = sl.meta
      meta.init_time = now
      meta.emited = 0
      meta.seq = si.seq
      meta.visited = si.visited
      meta.visited[rong.conf.name] = true
    end
  end
end

local notifs_merge = function (rong, notifs)
  local inv = rong.inv
  local conf = rong.conf
  local pending = rong.pending
  local ranking_find_replaceable = rong.ranking_find_replaceable
  
  local now=sched.get_time()
  
  --messages maintenance 
	for nid, n in pairs(inv) do
    local meta = n.meta
		if inv.own[nid] then
			if now - meta.init_time > conf.max_owning_time then
        log('FLOP', 'DEBUG', 'Purging old own notif: %s', tostring(nid))
				inv:del(nid)
			elseif meta.emited >= conf.max_ownnotif_transmits then
        log('FLOP', 'DEBUG', 'Purging own notif on transmit count: %s', tostring(nid))
				inv:del(nid)
			end
		else
			if meta.emited >= conf.max_notif_transmits then
        log('FLOP', 'DEBUG', 'Purging notif on transmit count: %s', tostring(nid))
				inv:del(nid)
			end
		end
	end

  for nid, data in pairs(notifs) do
		local ni=inv[nid]
		if ni then
      local meta = ni.meta
			meta.last_seen = now
			meta.seen=meta.seen+1
			pending:del(nid) --if we were to emit this, don't.
		else	
      inv:add(nid, data, false)
      rong.messages.init_notification(nid) --FIXME refactor?
      local n = inv[nid]
      n.meta.path=data._path
      data._path = nil
      
      -- signal arrival of new notification to subscriptions
      local matches=n.matches
      for sid, s in pairs(rong.view.own) do
        if matches[s] then
          log('FLOP', 'DEBUG', 'Singalling arrived notification: %s to %s'
            , tostring(nid), tostring(sid))
          sched.signal(s, n)
        end
      end
      
			--make sure table doesn't grow beyond inventory_size
			while inv:len()>conf.inventory_size do
				local mid=ranking_find_replaceable(rong)
				inv:del(mid or nid)
        log('FLOP', 'DEBUG', 'Inventory shrinking: %s, now %i long', 
          tostring(mid or nid), inv:len())
			end
		end
	end

end

local process_incoming_view = function (rong, view)
  --routing
  view_merge( rong, view )
  
  -- forwarding
  local matching = messaging.select_matching( rong, view )
  local pending, inv = rong.pending, rong.inv
  for mid, subs in pairs(matching) do
    local data = inv[mid].data
    local path = {}
    data._path = path
    for sid, s in pairs(subs) do
      for node, _ in pairs(s.meta.visited) do
        path[node] = true
      end
    end    
    rong.pending:add(mid, data)
  end
end

local process_incoming_notifs = function (rong, notifs)
  notifs_merge(rong, notifs)
end

M.new = function(rong)  
  local msg = {}
  
  local ranking_method = rong.conf.ranking_find_replaceable 
  or 'find_fifo_not_on_path'
  rong.ranking_find_replaceable = (require 'messages.flop.ranking')[ranking_method]
  
  msg.broadcast_view = function ()
    for k, v in pairs (rong.view.own) do
      v.meta.seq = v.meta.seq + 1
    end
    local view_emit = {}
    for sid, s in pairs (rong.view) do
      local meta = s.meta
      meta.emited = meta.emited + 1
      local sr = {
        filter = s.filter,
        seq = meta.seq,
        visited =  meta.visited,
      }
      view_emit[sid] = sr
    end
    
    local ms = assert(encode_f({view=view_emit})) --FIXME tamaño!
    log('FLOP', 'DEBUG', 'Broadcast view: %s', tostring(ms))
    rong.net:broadcast( ms )
  end
  
  msg.incomming = {
    view = process_incoming_view,
    notifs = process_incoming_notifs
  }
  
  msg.init_subscription = function (sid)
    local now = sched.get_time()
    local s = assert(rong.view[sid])
    local meta = s.meta
    meta.init_time = now
    meta.seq = 0
    meta.emited = 0
    meta.visited = {}
  end
    
  msg.init_notification = function (nid)
    local now = sched.get_time()
    local n = assert(rong.inv[nid])
    local meta = n.meta
    meta.init_time=now
    meta.last_seen=now
    meta.emited=0
    meta.seen=1
  end

  return msg
end

return M