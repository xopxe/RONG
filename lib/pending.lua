local M = {}

local sched = require 'lumen.sched'
local log = require 'lumen.log'
local encoder_lib = require 'lumen.lib.dkjson' --'lumen.lib.bencode'
local encode_f, decode_f = encoder_lib.encode, encoder_lib.decode

M.new = function (rong)
  local net = rong.net
  local delay_message_emit = rong.conf.delay_message_emit
  local p = {}
  return {
    add = function (_, nid, n)
      local pnid = p[nid]
      if pnid then
        pnid:kill()
      end
      p[nid] = sched.new_task(function()
        sched.sleep(delay_message_emit)
        local m = {}
        m[nid]=n
        local s = encode_f( {notifs=m} ) --FIXME tamaño!
          log('RONG', 'DEBUG', 'Broadcasting notification: %i bytes', #s)
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