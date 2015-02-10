-- rwalk protocol

local M = {}

local log = require 'lumen.log'
local sched = require 'lumen.sched'
local messaging = require 'rong.lib.messaging'
local encoder_lib = require 'lumen.lib.dkjson' --'lumen.lib.bencode'
local encode_f, decode_f = encoder_lib.encode, encoder_lib.decode
local selector = require 'lumen.tasks.selector'

local EVENT_TRANSMIT_TOKEN = {}

-- Process incomming view message
local view_merge = function(rong, vi)
  local now = sched.get_time()
  local view = rong.view
  local conf = rong.conf
    
  -- add all not already registered subscriptions
  for sid, si in pairs(vi.subs) do
    local sl = view[sid]
    if not sl then
      log('TRW', 'DETAIL', 'Merging subscription: %s', tostring(sid))
      view:add(sid, si.filter, false)
      sl = view[sid]
      sl.meta.last_seen = now
    end
  end
end

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
			meta.seen=meta.seen+1
		else	
      log('TRW', 'DEBUG', 'Merging notification: %s', tostring(nid))
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
      
      ---[[
      --FIXME ???
      -- if all matching subscrptios are own, can be removed from buffer safely
      local only_own = true
      for sid, s in pairs(view) do
        if matches[s] and not view_own[sid] then
          only_own = false
          break;
        end
      end
      if only_own then 
        log('TRW', 'DEBUG', 'Purging notification: %s', tostring(nid))
        inv:del(nid)
        --n.meta.delivered = true -- attribute checked when building a token
      end
      --]]
      
		end
	end
end

--in a task to insure atomicity
sched.sigrun ( {EVENT_TRANSMIT_TOKEN}, function (_, rong, view)
  local inv = rong.inv
  
  -- Open connection
  log('TRW', 'DEBUG', 'Client connecting to: %s:%s', 
    tostring(view.transfer_ip),tostring(view.transfer_port))
  local skt = selector.new_tcp_client(view.transfer_ip,view.transfer_port)
  
  -- Build token with everithing in buffer
  local notifs = {}
  for mid, m in pairs (inv) do
    --if not m.meta.delivered then -- see in notifs_merge()
      notifs[mid] = m.data 
    --end
  end
  local token = {
    token = rong.token,
    notifs = notifs,
  }
  local ms = assert(encode_f(token)) --FIXME tamaño!
  
  -- Send and then disconnect
  log('TRW', 'DETAIL', 'Client sending %i notifs, %i bytes', 
    inv:len(), #ms)  
  local ok, err, length = skt:send_sync(ms)
  skt:close()
  
  if ok then
    --token handled
    log('TRW', 'DETAIL', 'Handed over token: %s', tostring(rong.token))
    rong.token = nil
    rong.token_ts = nil
    
    --remove all handled notifiactions
    for mid, _  in pairs (notifs) do
      inv:del(mid)
    end
  else
    --token not handled
    log('TRW', 'DETAIL', 'Failed to hand over token %s with error %s (%i bytes sent)', 
      tostring(rong.token), tostring(err), length)
  end
end)

-- Get handler for reading a token from socket
local get_receive_token_handler = function (rong)
  return function(_, sktd, err)
    assert(sktd, err)
    log('TRW', 'DEBUG', 'Client accepted: %s', tostring(sktd.stream))
    -- sched.run( function() -- removed, only single client
    local chunks = {}
    repeat
      local chunk, err, err2 = sktd.stream:read()
      ---log('TRW', 'DEBUG', '>> %s', tostring(chunk))
      if chunk then chunks[#chunks+1] = chunk end
    until chunk == nil
    local sc = table.concat(chunks)
    log('TRW', 'DEBUG', 'Client received %i bytes', #sc)
    
    local token = decode_f(sc)
    if token then
      -- got token
      log('TRW', 'DETAIL', 'Got token: %s', tostring(token.token))
      notifs_merge(rong, token.notifs)
      rong.token = token.token
      rong.token_ts = sched.get_time()
    else
      -- failed to get token
      log('TRW', 'DETAIL', 'Failed to get token')
    end
    -- end)
  end
end

local process_incoming_view = function (rong, view)
  view_merge( rong, view )
  
  --FIXME, add selection logic
  --handle_token(rong, view)
  if rong.token then
    if sched.get_time() - rong.token_ts > (rong.conf.token_hold_time or 0) 
    and not view.token then
      --sched.run(transmit_token, rong, view)
      sched.signal (EVENT_TRANSMIT_TOKEN, rong, view)
    end
  end
end

M.new = function(rong)  
  local conf = rong.conf
  local msg = {}
  
  rong.token = conf.create_token
  rong.token_ts = sched.get_time()
  
  local tcp_server = selector.new_tcp_server(conf.listen_on_ip, conf.transfer_port, 0, 'stream')
  conf.listen_on_ip, conf.transfer_port = tcp_server: getsockname()
  log('TRW', 'INFO', 'Accepting connections on: %s:%s', 
    tostring(conf.listen_on_ip), tostring(conf.transfer_port)) 
  sched.sigrun({tcp_server.events.accepted}, get_receive_token_handler(rong))

  msg.broadcast_view = function ()
    local subs = {}
    local view_emit = {
      emitter = conf.name,
      transfer_ip = conf.listen_on_ip,
      transfer_port = conf.transfer_port,
      token = rong.token,
      subs = subs
    }
    for sid, s in pairs (rong.view) do
      local meta = s.meta
      local sr = {
        filter = s.filter,
      }
      subs[sid] = sr
    end   
    local ms = assert(encode_f({view=view_emit})) --FIXME tamaño!
    log('TRW', 'DEBUG', 'Broadcast view: %s', tostring(ms))
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
    meta.emited=0
    meta.seen=1
  end

  return msg
end

return M