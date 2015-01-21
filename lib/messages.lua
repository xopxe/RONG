local M = {}

local log = require 'lumen.log'
local sched = require 'lumen.sched'

--local broadcast = require 'lib.networking'.broadcast
--local ivs = require 'lib.inventory_view_sets'
--local messages, view, view_meta = ivs.messages, ivs.view, ivs.view_meta

local encoder_lib = require 'lumen.lib.dkjson' --'lumen.lib.bencode'
local encode_f, decode_f = encoder_lib.encode, encoder_lib.decode

local networking = require 'lib.networking'

local view_merge = function(rong, v, vi)
  local now = sched.get_time()

  for sid, s in pairs(vi) do
    log('RONG', 'DEBUG', 'merging subscription: %s', tostring(sid))
    local meta = rong.view_meta[sid]
    if not rong.view[sid] then
      v:add(sid, s)
      meta = rong.view_meta[sid]
      meta.ts = now
      --v[sid].visited[conf.name] = true
      s.visited[rong.conf.name] = true
    else
      local sl = v[sid]
      if sl.seq < s.seq then
        sl.seq = s.seq
      end    
    end
    meta.last_seen = now
  end
end


local process_incoming_view = function (rong, m)
  log('RONG', 'DEBUG', 'Incomming view: %s', tostring(m))
  
  --routing
  view_merge( rong, view, m.view )
  
  --[[
  -- forwarding
  local matching = messages.matching( m.view )
  local s = encode_f( matching ) --FIXME tamaño!
  broadcast( s )
  --]]
end

M.new_incomming_handler = function(rong)
  return function (sktd, data, err, part)
    if data then 
      log('RONG', 'DEBUG', 'Incomming data: %s', tostring(data))
      local m = decode_f(data)
      if m.view then
        process_incoming_view(rong, m)
      elseif m.messages then
      elseif m.subscribe then
      elseif m.subrequest then
      end
    else
      log('RONG', 'DEBUG', 'Incomming error: %s', tostring(err))
    end
    return true
  end
end

M.broadcast_view = function (rong)
  for k, v in pairs (rong.view:own()) do
    v.seq = v.seq + 1
  end
  local view_emit = {--[[emitter=assert(conf.name),--]] 
    view=rong.view, 
  }
  local s = assert(encode_f(view_emit)) --FIXME tamaño!
  log('RONG', 'DEBUG', 'Broadcast view: %s', tostring(s))
  networking.broadcast( s )
end

return M