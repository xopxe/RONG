local M = {}

local sched = require 'lumen.sched'
local log = require 'lumen.log'

M.new = function (rong)
  local net = rong.net
  local conf = rong.conf
  local encode_f, decode_f = conf.encode_f, conf.decode_f
  
  local p = {}
  return {
    add = function (_, nid, n)
      local pnid = p[nid]
      if pnid then
        pnid:kill()
      end
      p[nid] = sched.new_task(function()
        sched.sleep(conf.delay_message_emit * math.random())
        local m = {}
        m[nid]=n
        local s = encode_f( {notifs=m} ) --FIXME tamaño!
        --log('RONG', 'DEBUG', 'Broadcasting notification %s: %i bytes', s, #s)
        --log('RONG', 'DEBUG', 'Broadcasting notification: %i bytes', #s)
        rong.net:broadcast( s )
      end)
    end,
    
    del = function (_, nid)
      local pnid = p[nid]
      if pnid then
        pnid:kill()
      end
    end, 
  }
end


return M