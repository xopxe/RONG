local M = {}

local selector = require 'lumen.tasks.selector'
local sched = require 'lumen.sched'
local log = require 'lumen.log'

M.new = function(conf)  
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
    
  local messages = require 'lib.messages'.new(rong)
  
  -- start tasks
  --rong.broadcast_listener_task = sched.sigrun(
  --  { rong.signals.broadcast_view }, 
  --  function() messages:broadcast_view() end
  --)
  
  rong.broadcast_view_task = sched.run( 
    function ()
      while true do
        --sched.signal( rong.signals.broadcast_view )
        messages:broadcast_view()
        sched.sleep( conf.send_views_timeout )
      end
    end
  )

  M.subscribe = function (rong, sid, filter)
    --subscriptions:add(sid,{subscription_id=sid, filter=s.filter, p_encounter=p_encounter, 
    --last_seen=t, ts=t, cached_template=parser.build_subscription_template(s),own=skt})
    log('RONG', 'INFO', 'Publishing subscription: "%s" with filter %s',
      tostring(sid), tostring(filter))
    local now = sched.get_time()
    rong.view:add(sid, filter, true)
    local meta = rong.view_meta[sid]
    meta.ts = now
    meta.last_seen = now
    log('RONG', 'INFO', 'subscriptions: %i', rong.view:len())
  end 
 
  log('RONG', 'INFO', 'Library instance initialized: %s', tostring(rong))
  return rong
end

return M