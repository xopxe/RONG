-- ron protocol

local M = {}

local log = require 'lumen.log'
local sched = require 'lumen.sched'
local messaging = require 'rong.lib.messaging'

local queue_set = require "rong.lib.queue_set"
local seen_notifs = queue_set.new()

local pairs, ipairs, tostring, assert = pairs, ipairs, tostring, assert

local equal_filters = function (a, b)
  for k, v in pairs (a) do
    local vv=b[k]
    if not vv or v[1]~=vv[1] or v[2]~=vv[2] or v[3]~=vv[3] then return false end
  end
  for k, v in pairs (b) do
    local vv=a[k]
    if not vv or v[1]~=vv[1] or v[2]~=vv[2] or v[3]~=vv[3] then return false end
  end
  return true
end


local view_merge = function(rong, vi)
  local now = sched.get_time()  
  local conf, inv, view = rong.conf, rong.inv, rong.view
  local my_node = rong.conf.name

  for sid, si in pairs(vi) do
    if not view.own[sid] then 
      log('FLOP', 'DEBUG', 'Merging subscription: %s', tostring(sid))
      local sl = view[sid]
      local meta
      if not sl then
        sl = view:add(sid, si.filter, false)
        meta = sl.meta
        meta.q = {}
        meta.init_time = si.init_time
        meta.store_time = now
      end
      meta = sl.meta
      
      if not meta.seq or meta.seq < si.seq then     
        log('FLOP', 'DEBUG', 'Updating sub %s: seq %s-\>%i', tostring(sid), tostring(meta.seq), si.seq)
        meta.seq = si.seq
        meta.visited = {[my_node] = true}
        meta.last_seen = now
        
        if not equal_filters(sl.filter, si.filter) then
          log('FLOP', 'DEBUG', 'Updating filter to %s', table.concat(si.filter[1]))
          view:update(sid, si.filter)
        end
        
        for _, node in ipairs(si.visited) do
          log('FLOP', 'DEBUG', 'Updating sub %s: visited %s', tostring(sid), tostring(node))
          meta.visited[node] = true
          if node ~= my_node then 
            meta.q[node] = meta.q[node] or 0.5
          end
        end
        
        local matching = messaging.select_matching( rong, {[sid]=sl} )
        for _, mid in ipairs(matching) do
          if inv.own[mid] then -- solo para own?????? FIXME
            local path = {}
            local metaq = assert(meta.q)
            for node, _ in pairs(meta.visited) do
              if node ~= my_node then 
                local qold = metaq[node] or 0.5    
                metaq[node] = qold + (1-qold)*conf.q_reinf
              end
            end
            
            local sortq = {}
            for node, q in pairs(metaq) do 
              sortq[#sortq+1] = node 
              sortq[node] = true
            end 
            --[[
            for node, _ in pairs(inv[mid].meta.path) do 
              if not sortq[node] then 
                sortq[#sortq+1] = node
                sortq[node] = true
              end
            end
            --]]
            table.sort(sortq, function(a,b) return metaq[a]>metaq[b] end)
            log('FLOP', 'DEBUG', 'Updating subs %s for %s: sortq [%s]', sid, mid, table.concat(sortq,' '))
            local max = conf.max_path_count
            if max>#sortq then max=#sortq end
            for i=1, max do path[sortq[i]] = true end
            inv[mid].meta.path = path
          end
        end
      end
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

  for nid, inn in pairs(notifs) do  
    local ni=inv[nid]
    if ni then
      local meta = ni.meta
      meta.last_seen = now
      meta.seen=meta.seen+1
      meta.seen_on[inn.emmiter]=true
      for _, n in ipairs(inn.path) do meta.path[n]=true end
      pending:del(nid) --if we were to emit this, don't.
    else	
      if not seen_notifs:contains(nid) then
        seen_notifs:pushright(nid)
        while seen_notifs:len()>conf.max_notifid_tracked do
          seen_notifs:popleft()
        end

        inv:add(nid, inn.data, false)
        local n = rong.messages.init_notification(nid) --FIXME refactor?
        local meta = n.meta
        meta.init_time = inn.init_time
        meta.seen_on[inn.emmiter]=true
        for _, n in ipairs(inn.path) do meta.path[n]=true end

        -- signal arrival of new notification to subscriptions
        local matches=n.matches
        for sid, s in pairs(rong.view.own) do
          if matches[s] then
            log('FLOP', 'DEBUG', 'Signalling arrived notification: %s to %s'
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

end

local process_incoming_view = function (rong, view)
  local now = sched.get_time()
  local conf = rong.conf
  local my_node = rong.conf.name
  local attach = conf.attachments or {}

  --routing
  view_merge( rong, view.subs )

  -- forwarding
  local skipnotif = {}
  for _, nid in ipairs(view.skip or {}) do
    skipnotif[nid]= true
  end

  local matching = messaging.select_matching( rong, view.subs )
  local pending, inv = rong.pending, rong.inv
  table.sort(matching, function(ma, mb)
      return inv[ma].meta.init_time < inv[mb].meta.init_time
  end)
  for _, mid in ipairs(matching) do
    log('FLOP', 'DEBUG', 'Icomming view, found matching %s', tostring(mid))
    local m = inv[mid]
    local meta = m.meta
    if meta.has_attach and not attach[mid] then
      log('FLOP', 'DEBUG', '%s attach still not retrieved, skipping', tostring(mid))      
    else
      local outpath = {}
      for node, _ in pairs (meta.path) do 
        if node ~= my_node then 
          outpath[#outpath+1]=node
        end
      end
      if now-meta.last_seen>conf.message_inhibition_window and not skipnotif[mid] then
        meta.emited = meta.emited + 1 --FIXME do inside pending?
        pending:add(mid, {
          data=m.data, 
          path=outpath, 
          emitter=rong.conf.name, 
          has_attach = (attach[mid]~=nil),
          init_time=meta.init_time,
        })
      end
    end
  end
end

local process_incoming_notifs = function (rong, notifs)
  notifs_merge(rong, notifs)
end

local apply_aging = function (rong)
  local now = sched.get_time()
  local conf = rong.conf

  for sid, s in pairs(rong.view) do
    local meta = assert(s.meta)
    local q = assert(meta.q)
    for node, oldq in pairs(q) do
      q[node]=oldq * conf.q_decay^(now-meta.last_seen)
    end
    meta.last_seen=now
  end
end

M.new = function(rong)  
  local msg = {}
  local encode_f, decode_f = rong.conf.encode_f, rong.conf.decode_f

  local ranking_method = rong.conf.ranking_find_replaceable 
  or 'find_fifo_not_on_path'
  rong.ranking_find_replaceable = assert((require 'rong.messages.flop.ranking')[ranking_method])

  msg.broadcast_view = function ()
    apply_aging(rong)
    for k, v in pairs (rong.view.own) do
      v.meta.seq = v.meta.seq + 1
    end
    local subs = {}
    for sid, s in pairs (rong.view) do
      local meta = s.meta
      local outvisited = {}
      for node, _ in pairs(meta.visited) do outvisited[#outvisited+1] = node end
      local sr = {
        filter = s.filter,
        seq = meta.seq,
        visited = outvisited,
        init_time = meta.init_time,
      }
      subs[sid] = sr
    end

    local ms = assert(encode_f({view={subs=subs}})) --FIXME tamaño!

    if rong.conf.view_skip_list then
      local ms_candidate
      local skip = {}
      for mid, _ in pairs(rong.inv) do
        skip[#skip+1] = mid
        ms_candidate = assert(encode_f({view={subs=subs, skip=skip}})) 
        if #ms_candidate>1472 then break end
        ms = ms_candidate
      end
    end

    --log('FLOP', 'DEBUG', 'Broadcast view %s (%i bytes)', ms, #ms)
    log('FLOP', 'DEBUG', 'Broadcast view (%i bytes)', #ms)
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
    meta.store_time = now
    meta.last_seen = now
    meta.seq = 0
    meta.visited = {}
    meta.q={}
    return s
  end

  msg.init_notification = function (nid)
    local now = sched.get_time()
    local n = assert(rong.inv[nid])
    local meta = n.meta
    meta.init_time=now
    meta.store_time=now
    meta.last_seen=now
    meta.emited=0
    meta.seen=1
    meta.seen_on={}
    meta.path={}
    return n
  end

  return msg
end

return M