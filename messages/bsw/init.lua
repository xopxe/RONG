-- binary spray and wait protocol

local M = {}

local log = require 'lumen.log'
local sched = require 'lumen.sched'
local messaging = require 'rong.lib.messaging'
local selector = require 'lumen.tasks.selector'

local EVENT_TRIGGER_EXCHANGE = {}


local view_merge = function(rong, vi)
  local now = sched.get_time()
  local view = rong.view
  for sid, si in pairs(vi) do
    log('BSW', 'DEBUG', 'Merging subscription: %s', tostring(sid))
    if not view[sid] then
      view:add(sid, si.filter, false)
      local sl = view[sid]
      sl.meta.last_seen = now
    end
  end
end

-- Process incomming token
local notif_merge = function (rong, notif)
  local now = sched.get_time()
  local inv = rong.inv
  local view, view_own = rong.view, rong.view.own
   
  local nid, reg = next(notif)
  local ni=inv[nid]
  if ni then
    if ni.meta.hops > reg.hops then
      ni.meta.hops = reg.hops 
    end
  else	
    log('BSW', 'DEBUG', 'Merging notification: %s (%s copies)', 
      tostring(nid), tostring(reg.copies))
    inv:add(nid, reg.data, false)
    rong.messages.init_notification(nid) --FIXME refactor?
    local n = inv[nid]
    n.meta.hops = reg.hops
    n.meta.copies = reg.copies
    n.meta.init_time = reg.init_time

    -- signal arrival of new notification to subscriptions
    local matches=n.matches
    for sid, s in pairs(view_own) do
      if matches[s] then
        log('BSW', 'DEBUG', 'Singalling arrived notification: %s to %s', 
          tostring(nid), tostring(sid))
        sched.signal(s, n)
      end
    end
    
    if n.target then
      if n.target == rong.conf.name then
        log('BSW', 'DEBUG', 'Purging notification on destination: %s (%i copies)',
          tostring(nid). n.meta.copies)
        inv:del(nid)
      end
    elseif n.meta.hops >= rong.conf.max_hop_count then
      log('BSW', 'DEBUG', 'Purging notification on hop count: %s (%i copies)',
          tostring(nid). n.meta.copies)
      inv:del(nid)
    elseif n.meta.copies == 0 then
      log('BSW', 'WARN', 'Purging notification on wait mode: %s', tostring(nid))
      inv:del(nid)
    end
    
    log('BSW', 'DEBUG', 'Buffer size verification: %i, inventory_size=%s', 
      inv:len(), tostring(rong.conf.inventory_size))
    while inv:len() > rong.conf.inventory_size do
      local oldest, oldesttime = nil, math.huge
      for mid, m in pairs (inv) do
        if m.meta.init_time < oldesttime then
          oldest, oldesttime = mid, m.meta.init_time
        end
      end
      log('BSW', 'DEBUG', 'Purging notification on full buff: %s', tostring(oldest))
      inv:del(oldest)
    end
    
  end
end

--in a task to insure atomicity
sched.sigrun ( {EVENT_TRIGGER_EXCHANGE}, function (_, rong, view)
  local inv = rong.inv
  local encode_f, decode_f = rong.conf.encode_f, rong.conf.decode_f
  
  if inv:len()==0 then
    log('BSW', 'DEBUG', 'Sender not connecting, nothing to offer')
    return
  end

  -- Open connection
  log('BSW', 'DEBUG', 'Sender connecting to: %s:%s', 
    tostring(view.transfer_ip), tostring(view.transfer_port))
  local skt, err = selector.new_tcp_client(view.transfer_ip,view.transfer_port,
    nil, nil, 'line', 'stream', 5)
  
  if not skt then 
    log('BSW', 'DEBUG', 'Sender failed to connect: %s', err)
    return
  end
  
  --sched.sleep(1)
  skt.stream:set_timeout(5,5)
  
  -- send summary vector
  local sv = {} -- summary vector
  for mid, m in pairs (inv) do
      if m.meta.copies>1 then
        sv[#sv+1] = mid
      end
  end
  local svs = assert(encode_f({sv = sv}))
  
  log('BSW', 'DEBUG', 'Sender SV: %i notifs, %i bytes', 
    #sv, #svs+1)  
  local ok, errsend, length = skt:send_sync(svs..'\n')  
  if not ok then
    log('BSW', 'DEBUG', 'Sender SV send failed: %s', tostring(errsend))
    skt:close(); return
  end
  
  -- read request
  log('BSW', 'DEBUG', 'Sender REQ read started...')
  local reqs, errread = skt.stream:read()
  log('BSW', 'DEBUG', 'Sender REQ read finished: %s', tostring(reqs))
  if not reqs then
    log('BSW', 'DEBUG', 'Sender REQ read failed: %s', tostring(errread))
    skt:close(); return
  end
  
  -- send requested data
  local served_notif = {}
  local matching = messaging.select_matching( rong, view.subs )
  for _, mid in ipairs(matching) do
    local m = inv[mid]
    local out = {}
    out[mid] = {
      data = m.data,
      hops = m.meta.hops+1,
      copies = 0,
      init_time = m.meta.init_time,
    }
    local outs = assert(encode_f(out))
    served_notif[mid] = true
    log('BSW', 'DEBUG', 'Sender DATA direct delivery: %s, %i bytes', mid, #outs+1)      
    local okdata, errsenddata, lengthdata = skt:send_sync(outs..'\n')
    if okdata then
      log('BSW', 'DEBUG', 'Sender DATA delivered directly %s', tostring(mid))
      --inv:del(mid)
    else
      log('BSW', 'DEBUG', 'Sender DATA direct delivery failed: %s', tostring(errsenddata))
    end
  end
  
  local req = assert(decode_f(reqs))
  for _, mid in ipairs (req.req) do
    local m = inv[mid]
    if not m then -- could've been deleted since offered
      log('BSW', 'DEBUG', 'Got Request but was removed since offer: %s', tostring(mid))
    elseif not served_notif[mid] then
      local out = {}
      local transfer_copies = math.floor(m.meta.copies/2)
      out[mid] = {
        data = m.data,
        hops = m.meta.hops+1,
        copies = transfer_copies,
        init_time = m.meta.init_time,
      }
      local outs = assert(encode_f(out))
      log('BSW', 'DEBUG', 'Sender DATA built: %s, %i bytes', mid, #outs+1)      
      local okdata, errsenddata, lengthdata = skt:send_sync(outs..'\n')
      if okdata then
        m.meta.copies = m.meta.copies - transfer_copies
        log('BSW', 'DEBUG', 'Sender DATA transfered %i copies and kept %i for %s', 
          transfer_copies, m.meta.copies, tostring(mid))
      else
        log('BSW', 'DEBUG', 'Sender DATA transfer failed: %s', tostring(errsenddata))
        break;
      end
      served_notif[mid]=true
    end
  end
  
  skt:close()
end)

-- Get handler for reading a transfer from socket
local get_receive_transfer_handler = function (rong)
  local inv = rong.inv
  local encode_f, decode_f = rong.conf.encode_f, rong.conf.decode_f
  return function(_, skt, err)
    if skt==nil then
      log('BSW', 'ERROR', 'Socket error: %s', tostring(err))
      print(debug.traceback())
      os.exit()
    end
    
    log('BSW', 'DEBUG', 'Receiver accepted client: %s:%s', skt:getpeername())
    sched.run( function() -- removed, only single client
      --skt.stream:set_timeout(5,5)
      
      -- read summary vector
      local ssv, errread = skt.stream:read()
      if not ssv then
        log('BSW', 'DEBUG', 'Receiver SV read failed: %s', tostring(errread))
        skt:close(); return
      end
      local sv, parserr = decode_f(ssv)
      if not sv then
        log('BSW', 'DEBUG', 'Parse SV failed: %s', tostring(parserr))
        skt:close(); return
      end
      
       -- send request
      local req = {}
      for _, mid in ipairs(sv.sv) do
        if not inv[mid] then
          req[#req+1] = mid
        end
      end
      
      if #req == 0 then
        log('BSW', 'DEBUG', 'Receiver early close, nothing to request')
        skt:close(); return
      end
      
      local view_emit = {}
      local sreq = assert(encode_f({req = req}))
      
      log('BSW', 'DEBUG', 'Receiver REQ built: %i notifs, %i bytes', 
        #req, #sreq+1)  
      local ok, errsend, length = skt:send_sync(sreq..'\n')  
      if not ok then
        log('BSW', 'DEBUG', 'Receiver REQ send failed: %s', tostring(errsend))
        skt:close(); return
      end
      
      -- receive data
      repeat
        local data, decoderr
        log('BSW', 'DEBUG', 'Receiver DATA read started...')
        local sdata, errdataread = skt.stream:read()
        log('BSW', 'DEBUG', 'Receiver DATA read finished')
        if sdata then
          data, decoderr = decode_f(sdata)
          if data then
            notif_merge(rong, data)
          else
            log('BSW', 'DEBUG', 'Parse DATA read failed: %s', tostring(decoderr))
          end        
        elseif errdataread~='closed' then
          log('BSW', 'DEBUG', 'Receiver DATA read failed: %s', tostring(errdataread))
        end
      until not sdata or not data
      
      skt:close()
    end)
  end
end

local process_incoming_view = function (rong, view)
  view_merge( rong, view.subs )
  
  local conf = rong.conf
  local neighbor = rong.neighbor
  if neighbor[view.emitter] then
    -- restart timer
    log('BSW', 'DEBUG', 'Restarting neighborhood timer for: %s', tostring(view.emitter))
    sched.signal(neighbor[view.emitter])
  else
    -- create timer
    log('BSW', 'DEBUG', 'Creating neighborhood timer for: %s', tostring(view.emitter))
    local reg = {}
    neighbor[view.emitter] = reg
    reg.task = sched.run(function ()
      local waitd = {
        reg, 
        timeout = conf.neighborhood_window or 2*conf.send_views_timeout
      }
      repeat
        local ev = sched.wait(waitd)
      until ev == nil -- exit on timeout
      log('BSW', 'DEBUG', 'Killing neighborhood timer for: %s', tostring(view.emitter))
      neighbor[view.emitter].task = nil
      neighbor[view.emitter] = nil
    end)
    
    sched.signal (EVENT_TRIGGER_EXCHANGE, rong, view)
  end
end

M.new = function(rong)  
  local conf = rong.conf
  local inv = rong.inv
  local msg = {}

  rong.neighbor = {}

  
  local tcp_server = selector.new_tcp_server(conf.listen_on_ip, conf.transfer_port, 'line', 'stream')
  conf.listen_on_ip, conf.transfer_port = tcp_server: getsockname()
  log('BSW', 'INFO', 'Accepting connections on: %s:%s', 
    tostring(conf.listen_on_ip), tostring(conf.transfer_port)) 
  sched.sigrun({tcp_server.events.accepted}, get_receive_transfer_handler(rong))

  msg.broadcast_view = function ()
    local subs = {}
    for sid, s in pairs (rong.view.own) do
      local meta = s.meta
      local sr = {
        filter = s.filter,
        --p = meta.p,
      }
      subs[sid] = sr
    end
    local view_emit = {
      emitter = conf.name,
      transfer_ip = conf.listen_on_ip,
      transfer_port = conf.transfer_port,
      subs=subs,
    }
    
    local ms = assert(encode_f({view=view_emit})) --FIXME tamaño!
    log('BSW', 'DEBUG', 'Broadcast view %s: %i bytes', ms, #ms)

    rong.net:broadcast( ms )
  end
  
  msg.incomming = {
    view = process_incoming_view,
  }
  
  msg.init_subscription = function (sid)
    local now = sched.get_time()
    local s = assert(rong.view[sid])
    local meta = s.meta
    meta.init_time = now
  end
    
  msg.init_notification = function (nid)
    local now = sched.get_time()
    local n = assert(inv[nid])
    local meta = n.meta
    meta.init_time=now
    meta.last_seen=now
    meta.hops=0
    meta.copies = conf.start_copies
    
    log('BSW', 'DEBUG', 'Buffer size verification: %i, inventory_size=%s', 
      inv:len(), tostring(rong.conf.inventory_size))
    while inv:len() > rong.conf.inventory_size do
      local oldest, oldesttime = nil, math.huge
      for mid, m in pairs (inv) do
        if m.meta.init_time < oldesttime then
          oldest, oldesttime = mid, m.meta.init_time
        end
      end
      log('BSW', 'DEBUG', 'Purging notification on full buffer: %s (%i copies)', 
        tostring(oldest), inv[oldest].meta.copies)
      inv:del(oldest)
    end
    
    local neighbor = rong.neighbor
    for k, timer in pairs (neighbor) do
      timer.task:kill()
      neighbor[k].task = nil
      neighbor[k] = nil
    end
    
  end
  
  return msg
end

return M