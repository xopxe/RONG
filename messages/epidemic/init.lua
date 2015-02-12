-- rwalk protocol

local M = {}

local log = require 'lumen.log'
local sched = require 'lumen.sched'
local messaging = require 'rong.lib.messaging'
local encoder_lib = require 'lumen.lib.dkjson' --'lumen.lib.bencode'
local encode_f, decode_f = encoder_lib.encode, encoder_lib.decode
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
local notifs_merge = function (rong, notifs)
  local now = sched.get_time()
  local inv = rong.inv
  local view, view_own = rong.view, rong.view.own
  
  for nid, data in pairs(notifs) do
		local ni=inv[nid]
		if ni then
      local meta = ni.meta
			meta.last_seen = now
		else	
      log('EPIDEMIC', 'DEBUG', 'Merging notification: %s', tostring(nid))
      inv:add(nid, data, false)
      rong.messages.init_notification(nid) --FIXME refactor?
      local n = inv[nid]
      
      -- signal arrival of new notification to subscriptions
      local matches=n.matches
      for sid, s in pairs(view_own) do
        if matches[s] then
          log('RON', 'DEBUG', 'Singalling arrived notification: %s to %s', 
            tostring(nid), tostring(sid))
          sched.signal(s, n)          
        end
      end
      
      
      if n.target then
        if n.target == rong.conf.name then
          log('EPIDEMIC', 'DEBUG', 'Purging notification on destination: %s', tostring(nid))
          inv:del(nid)
        end
      else
        --[[
        --FIXME ???
        -- if all matching subscriptions are own, can be removed from buffer safely
        -- (there shouldn't be any, until support for flooding subs is (re)added)
        local only_own = true
        for sid, s in pairs(view) do
          if matches[s] and not view_own[sid] then
            only_own = false
            break;
          end
        end
        if only_own then 
          log('EPIDEMIC', 'DEBUG', 'Purging notification: %s', tostring(nid))
          inv:del(nid)
          --n.meta.delivered = true -- attribute checked when building a token
        end
        --]]
      end

		end
	end
end

--in a task to insure atomicity
sched.sigrun ( {EVENT_TRIGGER_EXCHANGE}, function (_, rong, view)
  local inv = rong.inv
  
  -- Open connection
  log('EPIDEMIC', 'DEBUG', 'Sender connecting to: %s:%s', 
    tostring(view.transfer_ip),tostring(view.transfer_port))
  local skt = selector.new_tcp_client(view.transfer_ip,view.transfer_port,
    nil, nil, 'line', 'stream')
  
  -- send summary vector
  local sv = {} -- summary vector
  for mid, m in pairs (inv) do
      sv[#sv+1] = mid
  end
  local svs = assert(encode_f({sv = sv}))
  
  log('EPIDEMIC', 'DEBUG', 'Sender SV: %i notifs, %i bytes', 
    #sv, #svs)  
  local ok, errsend, length = skt:send_sync(svs..'\n')  
  if not ok then
    log('EPIDEMIC', 'DEBUG', 'Sender SV send failed: %s', tostring(errsend))
    return;
  end
  
  -- read request
  local reqs, errread = skt.stream:read()
  if not reqs then
    log('EPIDEMIC', 'DEBUG', 'Sender REQ read failed: %s', tostring(errread))
    return;
  end
  
  -- send requested data
  local out = {}
  local req = assert(decode_f(reqs))
  for _, mid in ipairs (req.req) do
    out[mid] = inv[mid].data
  end
  
  local outs = assert(encode_f({notifs=out}))
  log('EPIDEMIC', 'DEBUG', 'Sender DATA built: %i notifs, %i bytes', 
    #req, #svs)  
  local okdata, errsenddata, lengthdata = skt:send_sync(outs..'\n')  
  if not okdata then
    log('EPIDEMIC', 'DEBUG', 'Sender DATA send failed: %s', tostring(errsenddata))
    return;
  end

  skt:close()
end)

-- Get handler for reading a token from socket
local get_receive_token_handler = function (rong)
  local inv = rong.inv
  return function(_, skt, err)
    assert(skt, err)
    log('EPIDEMIC', 'DEBUG', 'Receiver accepted: %s', tostring(skt.stream))
    -- sched.run( function() -- removed, only single client
      
    -- read summary vector
    local ssv, errread = skt.stream:read()
    if not ssv then
      log('EPIDEMIC', 'DEBUG', 'Receiver SV read failed: %s', tostring(errread))
      return true
    end
    local sv = assert(decode_f(ssv))

     -- send request
    local req = {}
    for _, mid in ipairs(sv.sv) do
      if not inv[mid] then
        req[#req+1] = mid
      end
    end
    
    local sreq = assert(encode_f({req = req}))
    
    log('EPIDEMIC', 'DEBUG', 'Receiver REQ built: %i notifs, %i bytes', 
      #req, #sreq)  
    local ok, errsend, length = skt:send_sync(sreq..'\n')  
    if not ok then
      log('EPIDEMIC', 'DEBUG', 'Receiver REQ send failed: %s', tostring(errsend))
      return true
    end
    
    -- receive data
    local sdata, errdataread = skt.stream:read()
    if not sdata then
      log('EPIDEMIC', 'DEBUG', 'Receiver DATA read failed: %s', tostring(errdataread))
      return true
    end
    local data = assert(decode_f(sdata))
    
    notifs_merge(rong, data)
    
    return true
    -- end)
  end
end

local process_incoming_view = function (rong, view)
  --view_merge( rong, view )
  sched.signal (EVENT_TRIGGER_EXCHANGE, rong, view)
end

M.new = function(rong)  
  local conf = rong.conf
  local msg = {}
  
  rong.token = conf.create_token
  rong.token_ts = sched.get_time()
  
  local tcp_server = selector.new_tcp_server(conf.listen_on_ip, conf.transfer_port, 'line', 'stream')
  conf.listen_on_ip, conf.transfer_port = tcp_server: getsockname()
  log('EPIDEMIC', 'INFO', 'Accepting connections on: %s:%s', 
    tostring(conf.listen_on_ip), tostring(conf.transfer_port)) 
  sched.sigrun({tcp_server.events.accepted}, get_receive_token_handler(rong))

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
    log('EPIDEMIC', 'DEBUG', 'Broadcast view: %s', tostring(ms))
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
    meta.last_seen = now
  end
    
  msg.init_notification = function (nid)
    local now = sched.get_time()
    local n = assert(rong.inv[nid])
    local meta = n.meta
    meta.init_time=now
    meta.last_seen=now
  end

  return msg
end

return M