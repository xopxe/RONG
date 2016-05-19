-- epidemic protocol

local M = {}

local log = require 'lumen.log'
local sched = require 'lumen.sched'
local messaging = require 'rong.lib.messaging'
local selector = require 'lumen.tasks.selector'

local EVENT_TRIGGER_EXCHANGE = {}


--[[
-- Process incomming view message
local view_merge = function(rong, vi)
  local now = sched.get_time()
  local view = rong.view
  local conf = rong.conf
    
  -- add all not already registered subscriptions
  for sid, si in pairs(vi.subs) do
    local sl = view[sid]
    if not sl then
      log('EPIDEMIC', 'DETAIL', 'Merging subscription: %s', tostring(sid))
      view:add(sid, si.filter, false)
      sl = view[sid]
      sl.meta.last_seen = now
    end
  end
end
--]]

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
    log('EPIDEMIC', 'DEBUG', 'Merging notification: %s', tostring(nid))
    inv:add(nid, reg.data, false)
    rong.messages.init_notification(nid) --FIXME refactor?
    local n = inv[nid]
    n.meta.hops = reg.hops
    n.meta.init_time = reg.init_time

    -- signal arrival of new notification to subscriptions
    local matches=n.matches
    for sid, s in pairs(view_own) do
      if matches[s] then
        log('EPIDEMIC', 'DEBUG', 'Singalling arrived notification: %s to %s', 
          tostring(nid), tostring(sid))
        sched.signal(s, n)          
      end
    end
    
    if n.target then
      if n.target == rong.conf.name then
        log('EPIDEMIC', 'DEBUG', 'Purging notification on destination: %s', tostring(nid))
        inv:del(nid)
      end
    elseif n.meta.hops >= rong.conf.max_hop_count then
      log('EPIDEMIC', 'DEBUG', 'Purging notification on hop count: %s', tostring(nid))
      inv:del(nid)
    end
    
    while inv:len() > (rong.conf.inventory_size or math.huge) do
      local oldest, oldesttime
      for mid, m in pairs (inv) do
        if m.meta.init_time < (oldesttime or math.huge) then
          oldest, oldesttime = mid, m.meta.init_time
        end
      end
      log('EPIDEMIC', 'DEBUG', 'Purging notification on full buff: %s', tostring(oldest))
      inv:del(oldest)
    end
    
  end
end

--in a task to insure atomicity
sched.sigrun ( {EVENT_TRIGGER_EXCHANGE}, function (_, rong, view)
  local inv = rong.inv
  local encode_f, decode_f = rong.conf.encode_f, rong.conf.decode_f
  
  if inv:len()==0 then
    log('EPIDEMIC', 'DEBUG', 'Sender not connecting, nothing to offer')
    return
  end
  
  -- Open connection
  log('EPIDEMIC', 'DEBUG', 'Sender connecting to: %s:%s', 
    tostring(view.transfer_ip),tostring(view.transfer_port))
  local skt, err = selector.new_tcp_client(view.transfer_ip,view.transfer_port,
    nil, nil, 'line', 'stream', 5)
   
  if not skt then 
    log('EPIDEMIC', 'DEBUG', 'Sender failed to connect: %s', err)
    return
  end
  
  skt.stream:set_timeout(5, 5)

  -- send summary vector
  local sv = {} -- summary vector
  for mid, m in pairs (inv) do
    sv[#sv+1] = mid
  end
  local svs = assert(encode_f({sv = sv}))
  
  log('EPIDEMIC', 'DEBUG', 'Sender SV: %i notifs, %i bytes', 
    #sv, #svs+1)  
  local ok, errsend, length = skt:send_sync(svs..'\n')  
  if not ok then
    log('EPIDEMIC', 'DEBUG', 'Sender SV send failed: %s', tostring(errsend))
    skt:close(); return
  end
  
  -- read request
  log('EPIDEMIC', 'DEBUG', 'Sender REQ read started...')
  local reqs, errread = skt.stream:read()
  log('EPIDEMIC', 'DEBUG', 'Sender REQ read finished: %s', tostring(reqs))
  if not reqs then
    log('EPIDEMIC', 'DEBUG', 'Sender REQ read failed: %s', tostring(errread))
    skt:close(); return
  end
  
  -- send requested data
  local req = assert(decode_f(reqs))
  for _, mid in ipairs (req.req) do
    local m = inv[mid]
    if not m then -- could've been deleted since offered
      log('EPIDEMIC', 'DEBUG', 'Got Request but was removed since offer: %s', tostring(mid))
    else   
      local out = {}
      out[mid] = {
        data = inv[mid].data,
        hops = inv[mid].meta.hops+1,
        init_time = m.meta.init_time,
      }
      local outs = assert(encode_f(out))
      log('EPIDEMIC', 'DEBUG', 'Sender DATA built: %s, %i bytes', mid, #outs+1)      
      local okdata, errsenddata, lengthdata = skt:send_sync(outs..'\n')  
      if not okdata then
        log('EPIDEMIC', 'DEBUG', 'Sender DATA transfer failed: %s', tostring(errsenddata))
        break;
      end
    end
  end
  
  skt:close()
end)

-- Get handler for reading a transfer from socket
local get_receive_transfer_handler = function (rong)
  local encode_f, decode_f = rong.conf.encode_f, rong.conf.decode_f
  local inv = rong.inv
  return function(_, skt, err)
    if skt==nil then
      log('EPIDEMIC', 'ERROR', 'Socket error: %s', tostring(err))
      print(debug.traceback())
      os.exit()
    end
    
    log('EPIDEMIC', 'DEBUG', 'Receiver accepted: %s:%s', skt:getpeername())
    sched.run( function() -- removed, only single client
      skt.stream:set_timeout(5, 5)
      
      -- read summary vector
      local ssv, errread = skt.stream:read()
      if not ssv then
        log('EPIDEMIC', 'DEBUG', 'Receiver SV read failed (%s): %s', skt:getpeername(), tostring(errread))
         skt:close(); return
      end
      local sv, parserr = decode_f(ssv)
      if not sv then
        log('EPIDEMIC', 'DEBUG', 'Parse SV failed (%s): %s', skt:getpeername(), tostring(parserr))
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
        log('EPIDEMIC', 'DEBUG', 'Receiver early close, nothing to request (%s)', skt:getpeername())
        skt:close(); return
      end
      
      local sreq = assert(encode_f({req = req}))
      
      log('EPIDEMIC', 'DEBUG', 'Receiver REQ built: %i notifs, %i bytes (%s:%s)', 
         #req, #sreq+1, skt:getpeername())
      local ok, errsend, length = skt:send_sync(sreq..'\n')  
      if not ok then
        log('EPIDEMIC', 'DEBUG', 'Receiver REQ send failed: %s (%s:%s)', 
          tostring(errsend), skt:getpeername())
        skt:close(); return
      end
      
      -- receive data
      repeat
        local data, decoderr
        local sdata, errdataread = skt.stream:read()
        if sdata then
          data, decoderr = decode_f(sdata)
          if data then
            notif_merge(rong, data)
          else
            log('EPIDEMIC', 'DEBUG', 'Parse DATA read failed (%s): %s',
              skt:getpeername(), tostring(decoderr))
          end        
        elseif errdataread~='closed' then
          log('EPIDEMIC', 'DEBUG', 'Receiver DATA read failed (%s): %s', 
            skt:getpeername(), tostring(errdataread))
        end
      until not sdata or not data
      
      skt:close()
    end)
  end
end

local process_incoming_view = function (rong, view)
  local conf = rong.conf
  local neighbor = rong.neighbor
  if neighbor[view.emitter] then
    -- restart timer
    sched.signal(neighbor[view.emitter])
  else
    -- create timer
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
      neighbor[view.emitter].task = nil
      neighbor[view.emitter] = nil
    end)
    
    sched.signal (EVENT_TRIGGER_EXCHANGE, rong, view)
  end
end

M.new = function(rong)  
  local conf = rong.conf
  local encode_f, decode_f = rong.conf.encode_f, rong.conf.decode_f

  local msg = {}

  rong.neighbor = {}

  
  local tcp_server = selector.new_tcp_server(conf.listen_on_ip, conf.transfer_port, 'line', 'stream')
  conf.listen_on_ip, conf.transfer_port = tcp_server: getsockname()
  log('EPIDEMIC', 'INFO', 'Accepting connections on: %s:%s', 
    tostring(conf.listen_on_ip), tostring(conf.transfer_port)) 
  sched.sigrun({tcp_server.events.accepted}, get_receive_transfer_handler(rong))

  msg.broadcast_view = function ()
    local subs = {}
    local view_emit = {
      emitter = conf.name,
      transfer_ip = conf.listen_on_ip,
      transfer_port = conf.transfer_port,
      --notifs = ???,
    }
    --[[
    for sid, s in pairs (rong.view) do
      local meta = s.meta
      local sr = {
        filter = s.filter,
      }
      subs[sid] = sr
    end
    --]]
    local ms = assert(encode_f({view=view_emit})) --FIXME tamaño!
    log('EPIDEMIC', 'DEBUG', 'Broadcast view %s: %i bytes', ms, #ms)

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
    local n = assert(rong.inv[nid])
    local meta = n.meta
    meta.init_time=now
    meta.last_seen=now
    meta.hops=0
    
    while rong.inv:len() > (rong.conf.inventory_size or math.huge) do
      local oldest, oldesttime
      for mid, m in pairs (rong.inv) do
        if m.meta.init_time < (oldesttime or math.huge) then
          oldest, oldesttime = mid, m.meta.init_time
        end
      end
      log('EPIDEMIC', 'DEBUG', 'Purging notification on full buff: %s', tostring(oldest))
      rong.inv:del(oldest)
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