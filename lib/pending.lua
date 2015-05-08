local M = {}

local sched = require 'lumen.sched'
local log = require 'lumen.log'
local encoder_lib = require 'lumen.lib.dkjson' --'lumen.lib.bencode'
local encode_f, decode_f = encoder_lib.encode, encoder_lib.decode

M.new = function (rong)
  local net = rong.net
  local conf = rong.conf
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
        log('RONG', 'DEBUG', 'Broadcasting notification %s: %i bytes', nid, #s)
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