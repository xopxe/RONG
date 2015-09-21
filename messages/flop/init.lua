-- ron protocol

local M = {}

local log = require 'lumen.log'
local sched = require 'lumen.sched'
local messaging = require 'rong.lib.messaging'

local queue_set = require "rong.lib.queue_set"
local seen_notifs = queue_set.new()
local selector = require 'lumen.tasks.selector'

local downloaders = {}

local pairs, ipairs, tostring, assert, math = pairs, ipairs, tostring, assert, math

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

local pick_server = function(seen_array)
  return seen_array[math.random(#seen_array)]
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
          log('FLOP', 'DETAIL', 'Updating sub %s: filter %s', tostring(sid), table.concat(si.filter[1]))
          view:update(sid, si.filter)
        end
        
        local metaq = assert(meta.q)
        
        for _, node in ipairs(si.visited) do
          if node ~= my_node then 
            log('FLOP', 'DETAIL', 'Updating sub %s: visited %s', 
              tostring(sid), tostring(node))
            meta.visited[node] = true
            metaq[node] = metaq[node] or 0.5 --FIXME change for avg q
          end
        end
        
        for node, _ in pairs(meta.visited) do
          if node ~= my_node then 
            local qold = assert(metaq[node]) -- or 0.5    
            metaq[node] = qold + (1-qold)*conf.q_reinf
          end
        end        
        
        --[[
        local matching = messaging.select_matching( rong, {[sid]=sl} )
        for _, mid in ipairs(matching) do
          if inv.own[mid] then -- solo para own?????? FIXME
            
           
            local sortq = {}
            for node, q in pairs(metaq) do 
              sortq[#sortq+1] = node 
              sortq[node] = true
            end 
            table.sort(sortq, function(a,b) return metaq[a]>metaq[b] end)
            log('FLOP', 'DETAIL', 'Updating sub %s for %s: sortq [%s]', sid, mid, table.concat(sortq,' '))
            local max = conf.max_path_count
            if max>#sortq then max=#sortq end
            local path = {}
            for i=1, max do path[ sortq[i] ] = true end
            inv[mid].meta.path = path
          end
        end
        --]]
      end
    end
  end
end

local http_downloader = function(rong, n)
  return function()
    local conf = assert(rong.conf)
    local meta = n.meta
    local nid = n.id
    local partial, partial_len = {}, 0
    repeat
      --pick one serv at random
      local seen_array = {}
      for k, v in pairs(meta.seen_on) do 
        seen_array[#seen_array+1]={node=k, address=v} 
      end
      while #seen_array==0 do
        sched.sleep(1)
        for k, v in pairs(meta.seen_on) do 
          seen_array[#seen_array+1]={node=k, address=v} 
        end
      end
      local serv = assert(pick_server(seen_array))
     
      log('FLOP', 'DETAIL', 'Requesting %s to %s:%s', nid..'?s='..(partial_len+1), 
        tostring(serv.address.ip), tostring(serv.address.port))
      local skt, err = selector.new_tcp_client(serv.address.ip, serv.address.port, 0, 0, -1, 'stream')
      if not skt then
        log('FLOP', 'DETAIL', 'Failed to connect to %s:%s with %s, will retry', 
          tostring(serv.address.ip), tostring(serv.address.port), tostring(err))
        sched.sleep(1)
      else
        skt.stream:set_timeout(conf.http_get_timeout)
        skt:send_sync('GET '..nid..'?s='..(partial_len+1)..' HTTP/1.1\r\n\r\n')
        local header = ''
        local start
        repeat
          local data, err, part = skt.stream:read() --meta.has_attach-#partial)
          header = header .. (data or '')
          start = header:match('\r\n\r\n(.*)$')
          --print ('!!!!!', data, err, header, start)
        until start or not data
        if start then
          local errcode = assert(tonumber(header:match('^%S+ (%d+) ')))
          if errcode~=200 then
            log('FLOP', 'DETAIL', 'got err %i on getting %s, removing %s as server', 
              errcode, nid, serv.node)
            meta.seen_on[serv.node] = nil
            start = nil
          end
          
          if start then
            partial[#partial+1], partial_len = start, partial_len+#start
            log('FLOP', 'DEBUG', 'http download started for %s', nid)
            while partial_len<meta.has_attach do
              local data, err = skt.stream:read() --meta.has_attach-#partial)
              --print ('+++++', nid, data and true, err, #(data or ''))
              if data then 
                log('FLOP', 'DEBUG', 'Succesfull GET fragment %s from %s (got %i+%i bytes)', 
                  nid, serv.node, partial_len, #data)
                partial[#partial+1], partial_len = data, partial_len+#data
              else
                log('FLOP', 'DEBUG', 'Failed GET fragment %s with "%s" from %s (got %i bytes)', 
                  nid, tostring(err), serv.node, partial_len)
                break
              end                  
            end
          end
        end
        skt:close() 
        if partial_len<meta.has_attach then
          log('FLOP', 'DETAIL', 'Failed attach GET %s from %s (got %i bytes), will retry', 
            nid, serv.node, partial_len)
          sched.sleep(1)
        end
      end
    until partial_len==meta.has_attach
    log('FLOP', 'DETAIL', 'Succesfull GET %s', nid)
    conf.attachments[nid]=table.concat(partial)
    -- signal arrival of new notification to subscriptions
    local matches=n.matches
    for sid, s in pairs(rong.view.own) do
      if matches[s] then
        log('FLOP', 'DEBUG', 'Signalling arrived notification w/attach: %s to %s'
          , tostring(nid), tostring(sid))
        sched.signal(s, n)
      end
    end
    
    ---[[
    -- gratuitous announcement, just because we downloaded it.
    log('FLOP', 'DETAIL', 'PostGET Going to broadcast %s (emited %i times)',
      tostring(nid), meta.emited)
    meta.emited = meta.emited + 1 --FIXME do inside pending?
    local outpath = {}
    local attach = conf.attachments or {}
    for node, _ in pairs (meta.path) do 
      if node ~= rong.conf.name then 
        outpath[#outpath+1]=node
      end
    end
    rong.pending:add(nid, {
      data=n.data, 
      path=outpath, 
      emitter=rong.conf.name, 
      has_attach = attach[nid] and #attach[nid],
      attach_on = attach[nid] and (conf.http_conf or {}),
      init_time=meta.init_time,
    }) 
    --]]
    
    downloaders[nid]=nil
  end
end

local notifs_merge = function (rong, notifs)
  local inv = rong.inv
  local conf = rong.conf
  local pending = rong.pending
  local ranking_find_replaceable = rong.ranking_find_replaceable
  local attach = conf.attachments or {}

  local now=sched.get_time()

  --messages maintenance 
  for nid, n in pairs(inv) do
    local meta = n.meta
    if inv.own[nid] then
      if now - meta.init_time > conf.max_owning_time then
        log('FLOP', 'DETAIL', 'Purging old own notif: %s', tostring(nid))
        inv:del(nid)
        attach[nid]=nil
        if downloaders[nid] then downloaders[nid]:kill(); downloaders[nid]=nil end
      elseif meta.emited >= conf.max_ownnotif_transmits then
        log('FLOP', 'DETAIL', 'Purging own notif on transmit count: %s', tostring(nid))
        inv:del(nid)
        attach[nid]=nil
        if downloaders[nid] then downloaders[nid]:kill(); downloaders[nid]=nil end
      end
    else
      if meta.emited >= conf.max_notif_transmits then
        log('FLOP', 'DETAIL', 'Purging notif on transmit count: %s', tostring(nid))
        inv:del(nid)
        attach[nid]=nil
        if downloaders[nid] then downloaders[nid]:kill(); downloaders[nid]=nil end
      end
    end
  end

  for nid, inn in pairs(notifs) do  
    local ni=inv[nid]
    if ni then
      local meta = ni.meta
      meta.last_seen = now
      meta.seen=meta.seen+1
      if not meta.seen_on[inn.emitter] then
        log('FLOP', 'DETAIL', 'Notif %s seen on %s (%s:%s)', nid, inn.emitter,
          tostring(inn.attach_on.ip), tostring(inn.attach_on.port))
        meta.seen_on[inn.emitter]=inn.attach_on
      end
      for _, n in ipairs(inn.path) do meta.path[n]=true end
      pending:del(nid) --if we were to emit this, don't.
    else	
      if not seen_notifs:contains(nid) then
        seen_notifs:pushright(nid)
        while seen_notifs:len()>conf.max_notifid_tracked do
          seen_notifs:popleft()
        end
        
        log('FLOP', 'DETAIL', 'New Notif %s seen on %s', nid, inn.emitter)
        inv:add(nid, inn.data, false)
        local n = rong.messages.init_notification(nid) --FIXME refactor?
        local meta = n.meta
        meta.init_time = inn.init_time
        meta.has_attach=inn.has_attach
        meta.seen_on[inn.emitter]=inn.attach_on
        for _, nid in ipairs(inn.path) do meta.path[nid]=true end
               
        -- signal arrival of new notification to subscriptions
        local matches=n.matches
        for sid, s in pairs(rong.view.own) do
          if matches[s] then
            if meta.has_attach and not downloaders[nid] then
              log('FLOP', 'DETAIL', 'Notif %s has attached %s bytes', nid, tostring(meta.has_attach)) 
              downloaders[nid] = sched.run(http_downloader(rong, n)) --ONLY DOWNLOADS FOR OWN
            else
              log('FLOP', 'INFO', 'Signalling arrived notification: %s to %s'
                , tostring(nid), tostring(sid))
              sched.signal(s, n)
            end
          end
        end
        
        --make sure table doesn't grow beyond inventory_size
        if inv:len()>conf.inventory_size then
          local mid=ranking_find_replaceable(rong)
          local to_remove_id = mid or nid
          
          inv:del(to_remove_id)
          attach[to_remove_id]=nil
          if downloaders[to_remove_id] then downloaders[to_remove_id]:kill(); downloaders[to_remove_id]=nil end
          log('FLOP', 'DEBUG', 'Inventory shrinking: %s, now %i long', 
            tostring(to_remove_id), inv:len())
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
      
      --/////////
      if inv.own[mid] then
        local sortq = {}
        for s, v in pairs(m.matches) do
          if v then 
            local metaq = assert(s.meta.q)
            for node, q in pairs(metaq) do 
              sortq[#sortq+1] = node 
              sortq[node] = q
            end 
          end
        end
        table.sort(sortq, function(a,b) return sortq[a]>sortq[b] end)
        local max = conf.max_path_count
        if max>#sortq then max=#sortq end
        local path = {}
        for i=1, max do path[ sortq[i] ] = true end
        meta.path = path      
      end
      --/////////
      
      local outpath = {}
      for node, _ in pairs (meta.path) do 
        if node ~= my_node then 
          outpath[#outpath+1]=node
        end
      end
      if now-meta.last_seen>conf.message_inhibition_window 
      and not skipnotif[mid] then
        log('FLOP', 'DETAIL', 'Going to broadcast %s (emited %i times)',
          tostring(mid), meta.emited)
        meta.emited = meta.emited + 1 --FIXME do inside pending?
        pending:add(mid, {
          data=m.data, 
          path=outpath, 
          emitter=rong.conf.name, 
          has_attach = attach[mid] and #attach[mid],
          attach_on = attach[mid] and (conf.http_conf or {}),
          init_time=meta.init_time,
        })
      end
      --print ('RRRRRRRRR', mid, attach[mid] and #attach[mid])
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


local start_http_server = function(rong)
  local conf = rong.conf
  local http_server = require "lumen.tasks.http-server"
  --http_server.HTTP_TIMEOUT = 1
  local downlcount =  {}
  sched.sigrun({http_server.EVENT_DATA_TRANSFERRED}, function(_, path, code, done, err, bytes)
    if code==200 and done then 
      downlcount[path] = (downlcount[path] or 0) + 1
      if downlcount[path]>=(conf.max_chunk_downloads or math.huge) then
        log('FLOP', 'DETAIL', 'Purging chunk %s on GET count', path)
        conf.attachments[path] = nil
        rong.inv:del(path)
      end
    end
  end)
  http_server.serve_static_content_from_table('/', assert(conf.attachments))
  http_server.init(conf.http_conf)
end
  
  
M.new = function(rong)  
  local msg = {}
  local encode_f, decode_f = rong.conf.encode_f, rong.conf.decode_f
  pick_server = rong.conf.pick_server or pick_server

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

    repeat
      local ms = assert(encode_f({view={subs={}}})) 
      local outsub = {}
      for sid, sr in pairs(subs) do
        outsub[sid] = sr
        local ms_candidate = assert(encode_f({view={subs=outsub}}))
        --print ('XXX', sid, ms_candidate)
        if #ms_candidate>1472 then 
          log('FLOP', 'WARN', 'View too long, splitting at %i bytes', #ms)      
          break 
        end
        ms = ms_candidate
        subs[sid] = nil
      end
      
      assert(not( next(outsub)==nil and next(subs)~=nil), 'Failed to split view!' )
      
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
    until next(subs) == nil
    
  end

  msg.incomming = {
    view = process_incoming_view,
    notifs = process_incoming_notifs,
  }

  msg.init_subscription = function (sid)
    local now = sched.get_time()
    local s = assert(rong.view[sid])
    local meta = s.meta
    meta.init_time = now
    meta.store_time = now
    meta.last_seen = now
    meta.seq = 0
    --meta.visited = {[rong.conf.name] = true}
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
    seen_notifs:pushright(nid)
    return n
  end
  
  msg.message_scan_for_delivery = function ()
    log('FLOP', 'DETAIL', 'Reviewing already stored messages')
    for nid, n in pairs(rong.inv) do
      local matches=n.matches
      local meta = n.meta
      for sid, s in pairs(rong.view.own) do
        if matches[s] then
          if meta.has_attach and not downloaders[nid] then
            log('FLOP', 'DEBUG', 'Notif %s has attached %s bytes', nid, tostring(meta.has_attach)) 
            downloaders[nid] = sched.run(http_downloader(rong, n)) --ONLY DOWNLOADS FOR OWN
          else
            log('FLOP', 'INFO', 'Signalling arrived notification: %s to %s'
              , tostring(nid), tostring(sid))
            sched.signal(s, n)
          end
        end
      end
    end
  end

  if rong.conf.attachments then 
    start_http_server(rong)
  end
    
  return msg
end

return M