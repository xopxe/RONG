local M = {}

local log = require 'lumen.log'
local sched = require 'lumen.sched'
local encoder_lib = require 'lumen.lib.dkjson' --'lumen.lib.bencode'
local encode_f, decode_f = encoder_lib.encode, encoder_lib.decode


local view_merge = function(rong, vi)
  local now = sched.get_time()
  
  local v=rong.view
  
  for sid, s in pairs(vi) do
    log('RONG', 'DEBUG', 'Merging subscription: %s', tostring(sid))
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
        --FIXME??
        sl.visited = s.visited
        sl.visited[rong.conf.name] = true
        --/FIXME??
      end    
    end
    meta.last_seen = now
  end
end



local process_incoming_view = function (rong, view)
  --routing
  view_merge( rong, view )
  
  --[[
  -- forwarding
  local matching = messages.matching( m.view )
  local s = encode_f( matching ) --FIXME tamaño!
  broadcast( s )
  --]]
end


M.new = function(rong)
  local msg = {}
  
  msg.broadcast_view = function ()
    for k, v in pairs (rong.view:own()) do
      v.seq = v.seq + 1
    end
    local view_emit = {--[[emitter=assert(conf.name),--]] 
      view=rong.view, 
    }
    local s = assert(encode_f(view_emit)) --FIXME tamaño!
    log('RONG', 'DEBUG', 'Broadcast view: %s', tostring(s))
    rong.net:broadcast( s )
  end
  
  msg.incomming = {
    view = process_incoming_view,
  }
  
  return msg
end

return M