local M = {}

local selector = require 'lumen.tasks.selector'
local sched = require 'lumen.sched'
local log = require 'lumen.log'

local function script_path()
   local str = debug.getinfo(2, "S").source:sub(2)
   return str:match("(.*/)")
end

M.new = function(conf)  
  local encoder_lib = require( conf.encoder or 'lumen.lib.dkjson')
  conf.encode_f = conf.encode_f or encoder_lib.encode
  conf.decode_f = conf.decode_f or encoder_lib.decode
  local encode_f = conf.encode_f
  local decode_f = conf.decode_f

  local ivs = assert(loadfile (script_path()..'lib/inventory_view_sets.lua'))()

  --M.conf = conf
  local rong = setmetatable({
    conf = conf,
    signals = loadfile (script_path()..'lib/signals.lua')(),
    inv = ivs.inv,
    view = ivs.view,
  }, {
    __index = M,
  })
  local messages = assert(require ('rong.messages.'..conf.protocol)).new(rong)
  rong.messages = messages
  
  local incomming_handler = function (data, err)
    if data then 
      --log('RONG', 'DEBUG', 'Incomming: %s', tostring(data))
      local m = assert(decode_f(data))
      for k, v in pairs(m) do
        if messages.incomming[k] then 
          --log('RONG', 'DEBUG', ' Incomming found: %s', tostring(k))
          messages.incomming[k] (rong, v)
        else
          log('RONG', 'WARN', ' Incomming unknown: %s', tostring(k))
        end
      end
      --[[
      if m.view then
        process_incoming_view(rong, m)
      elseif m.messages then
      elseif m.subscribe then
      elseif m.subrequest then
      end
      --]]
    else
      log('RONG', 'WARN', 'Incomming error: %s', tostring(err))
    end
    return true
  end
  rong.net = require 'rong.lib.networking'.new(rong, incomming_handler)
  rong.pending = require 'rong.lib.pending'.new(rong)
    
 
  -- start tasks
  --rong.broadcast_listener_task = sched.sigrun(
  --  { rong.signals.broadcast_view }, 
  --  function() messages:broadcast_view() end
  --)
  
  rong.broadcast_view_task = sched.run( 
    function ()
      local timeout = conf.send_views_timeout
      local spread = 0.1
      local spread_2 = spread / 2
      while true do
        --sched.signal( rong.signals.broadcast_view )
        messages:broadcast_view()
        sched.sleep( timeout + (spread*math.random()-spread_2)*timeout )
      end
    end
  )

  M.subscribe = function (rong, sid, filter)
    sid = sid or 'sid:'..math.random(2^31)
    --subscriptions:add(sid,{subscription_id=sid, filter=s.filter, p_encounter=p_encounter, 
    --last_seen=t, ts=t, cached_template=parser.build_subscription_template(s),own=skt})
    log('RONG', 'INFO', 'Publishing subscription: "%s"',
      tostring(sid))
    rong.view:add(sid, filter, true)
    messages.init_subscription(sid)
    if messages.message_scan_for_delivery then 
      messages.message_scan_for_delivery()
    end
    log('RONG', 'INFO', '  subscriptions: %i', rong.view:len())
    return rong.view[sid]
  end
  
  M.update_subscription = function (rong, sid, filter)
    if not sid then return nil, 'Missing sid for update' end
    log('RONG', 'INFO', 'Updating subscription: "%s"',
      tostring(sid))
    rong.view:update(sid, filter, true)
    --messages.init_subscription(sid)
    if messages.message_scan_for_delivery then 
      messages.message_scan_for_delivery()
    end
    return rong.view[sid]
  end
  
  M.notificate = function (rong, nid, data)
    nid = nid or 'nid:'..math.random(2^31)
    log('RONG', 'INFO', 'Publishing notification: "%s"',
      tostring(nid))
    rong.inv:add(nid, data, true)
    local entry = messages.init_notification(nid)
    if messages.message_scan_for_delivery then 
      messages.message_scan_for_delivery()
    end
    log('RONG', 'INFO', '  notifications: %i', rong.inv:len())
    return entry
  end

 
  log('RONG', 'INFO', 'Library instance initialized: %s', tostring(rong))
  return rong
end

return M