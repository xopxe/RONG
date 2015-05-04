-- trw protocol

local M = {}

local log = require 'lumen.log'
local sched = require 'lumen.sched'
local messaging = require 'rong.lib.messaging'
local encoder_lib = require 'lumen.lib.dkjson' --'lumen.lib.bencode'
local encode_f, decode_f = encoder_lib.encode, encoder_lib.decode
local selector = require 'lumen.tasks.selector'

local EVENT_TRANSMIT_TOKEN = {}

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
      log('TRW', 'DETAIL', 'Merging subscription: %s', tostring(sid))
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
      log('TRW', 'DEBUG', 'Merging notification: %s', tostring(nid))
      inv:add(nid, data, false)
      rong.messages.init_notification(nid) --FIXME refactor?
      local n = inv[nid]
      
      -- signal arrival of new notification to subscriptions
      local matches=n.matches
      for sid, s in pairs(view_own) do
        if matches[s] then
          log('TRW', 'DEBUG', 'Singalling arrived notification: %s to %s', 
            tostring(nid), tostring(sid))
          sched.signal(s, n)          
        end
      end
      
      
      if n.data.target then
        if n.data.target == rong.conf.name then
          log('TRW', 'DEBUG', 'Purging notification on destination: %s', tostring(nid))
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
          log('TRW', 'DEBUG', 'Purging notification: %s', tostring(nid))
          inv:del(nid)
          --n.meta.delivered = true -- attribute checked when building a token
        end
        --]]
      end

		end
	end
end

--in a task to insure atomicity
sched.sigrun ( {EVENT_TRANSMIT_TOKEN}, function (_, rong, view)
  local inv = rong.inv
  
  -- Open connection
  log('TRW', 'DEBUG', 'Sender connecting to: %s:%s', 
    tostring(view.transfer_ip),tostring(view.transfer_port))
  local skt, errconn = selector.new_tcp_client(view.transfer_ip,view.transfer_port,
    nil, nil, 'line', 'stream')
  
  if not skt then 
    log('TRW', 'DEBUG', 'Sender failed to connect: %s', errconn)
    return
  end
  
  skt.stream:set_timeout(5,5)

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
  local ms = assert(encode_f(token))
  
  -- Send and then disconnect
  log('TRW', 'DETAIL', 'Sender sending token %s, %i notifs, %i bytes', 
    token.token, inv:len(), #ms+1)  
  local ok, err, length = skt:send_sync(ms..'\n')
  
  if ok then
    --wait for a confirmation  (will accept anything)
    --print ('?????', ok, err, length)
    local line, err, err2 = skt.stream:read()
    --print ('!!!!!',line, err, err2 )
    
    if line then 
      --token handled
      log('TRW', 'DETAIL', 'Sender handed over token: %s', tostring(rong.token))
      rong.token = nil
      rong.token_ts = nil
      
      --remove all handled notifiactions
      for mid, _  in pairs (notifs) do
        inv:del(mid)
      end
    else
      log('TRW', 'DETAIL', 'Sender failed reading transfer confirmation: %s', err)
    end
  else
    --token not handled
    log('TRW', 'DETAIL', 'Sender failed to send token with error %s (%i bytes sent)', 
      tostring(err), length)
  end
  skt:close()
end)

-- Get handler for reading a token from socket
local get_receive_token_handler = function (rong)
  return function(_, sktd, err)
    assert(sktd, err)
    if rong.token and rong.token  then
      log('TRW', 'DEBUG', 'Already got a token, refusing client: %s:%s', sktd:getpeername())
      sktd:close()
      return
    end
    
    log('TRW', 'DEBUG', 'Receiver accepted client: %s:%s', sktd:getpeername())
    --sched.run( function() -- removed, only single client
      sktd.stream:set_timeout(5,5)
      
      local sc = ''
      local token
      repeat
        local chunk, err, err2 = sktd.stream:read()
        --log('TRW', 'DEBUG', 'Received chunk "%s" "%s" "%s"',
        --  tostring(chunk), tostring(err), tostring(err2))
        if chunk then
          sc = sc..chunk
          token = decode_f(sc)
        end
      until sc or not chunk
      log('TRW', 'DEBUG', 'Receiver got transfer (%i bytes)', #sc)
      
      if token then
        -- got token
        log('TRW', 'DETAIL', 'Receiver parsed token: %s', tostring(token.token))
        notifs_merge(rong, token.notifs)
        rong.token = token.token
        rong.token_ts = sched.get_time()
        sktd:send_async(encode_f({accepted=true})..'\n')
      else
        -- failed to get token
        log('TRW', 'DETAIL', 'Failed to get token')
      end
    --sktd:close() --commented because of confirmation retries... ????
    --end)
  end
end

local process_incoming_view = function (rong, view)
  local conf = rong.conf
  
  --[[
  -- UNTESTED select a neighbor from a list of seen in a time window 
  if sched.get_time() - rong.token_ts > (conf.token_hold_time or 0) 
    and not view.token then
    local neighbor = rong.neighbor
    if neighbor[view.emitter] then
      -- restart timer
      log('TRW', 'DEBUG', 'Restarting neighborhood timer for: %s', tostring(view.emitter))
      sched.signal(neighbor[view.emitter])
    else
      -- create timer
      log('TRW', 'DEBUG', 'Creating neighborhood timer for: %s', tostring(view.emitter))
      local reg = {view=view}
      neighbor[view.emitter] = reg
      reg.task = sched.run(function ()
        local waitd = {
          reg, 
          timeout = conf.neighborhood_window or 2*conf.send_views_timeout
        }
        repeat
          local ev = sched.wait(waitd)
        until ev == nil -- exit on timeout
        log('TRW', 'DEBUG', 'Killing neighborhood timer for: %s', tostring(view.emitter))
        neighbor[view.emitter].task = nil
        neighbor[view.emitter] = nil
      end)
      
      local candidate = next(neighbor) --random selection in table
      sched.signal (EVENT_TRANSMIT_TOKEN, rong, candidate.view)
    end
  end
  --]]
  
  ---[[
  -- stochastic selection of neighbor
  if rong.token then
    log('TRW', 'DEBUG', 'Incomming view from %s', view.emitter)
    if sched.get_time() - rong.token_ts > (conf.token_hold_time or 0) 
    and not view.token then
      --sched.run(transmit_token, rong, view)
      sched.signal (EVENT_TRANSMIT_TOKEN, rong, view)
    end
  end
  --]]
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
    if rong.token then return end --!!!!!!
    
    local subs = {}
    local view_emit = {
      emitter = conf.name,
      transfer_ip = conf.listen_on_ip,
      transfer_port = conf.transfer_port,
      token = rong.token,
      --subs = subs
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
  end

  return msg
end

return M