local M = {}

local selector = require 'lumen.tasks.selector'
local sched = require 'lumen.sched'
local log = require 'lumen.log'

M.new = function(conf)
  
  local networking = require 'lib.networking'
  local messages = require 'lib.messages'
  local ivs = loadfile 'lib/inventory_view_sets.lua'()

  --M.conf = conf
  local rong = setmetatable({
    conf = conf,
    signals = loadfile 'lib/signals.lua'(),
    inventory = ivs.inventory,
    view = ivs.view,
    view_meta = ivs.view_meta,
  }, {
    __index = M,
  })
    
  networking.build_socket(rong, messages.new_incomming_handler(rong))
  
  -- start tasks
  sched.sigrun({ rong.signals.broadcast_view }, function()
    messages.broadcast_view(rong)
  end)
  
  sched.run( function ()
      while true do
        sched.signal( rong.signals.broadcast_view )
        sched.sleep( conf.send_views_timeout )
      end
  end)

  M.subscribe = function (rong, sid, filter)
    --subscriptions:add(sid,{subscription_id=sid, filter=s.filter, p_encounter=p_encounter, 
    --last_seen=t, ts=t, cached_template=parser.build_subscription_template(s),own=skt})
    log('RONG', 'INFO', 'Publishing subscription: "%s" with filter %s', tostring(sid), tostring(filter))
    local now = sched.get_time()
    rong.view:add(sid, filter, true)
    local meta = rong.view_meta[sid]
    meta.ts = now
    meta.last_seen = now
    log('RONG', 'INFO', 'subscriptions: %i', rong.view:len())
  end 
 
  log('RONG', 'INFO', 'Instance initialized: %s', tostring(rong))
  return rong
end

return M