local M = {}

local log = require 'lumen.log'
local selector = require 'lumen.tasks.selector'

local udp_in, udp_out

--local udp = selector.new_udp(conf.udp.broadcast or '255.255.255.255', 
--conf.port, conf.ip, conf.port, -1, messages.incomming_handler)
M.build_socket = function( rong, handler )
  log('RONG', 'INFO', 'UDP listening on %s:%s', 
    tostring(rong.conf.listen_on_ip), 
    tostring(rong.conf.protocol_port))
  udp_in = assert(selector.new_udp(nil, nil, rong.conf.listen_on_ip, rong.conf.protocol_port, -1, handler))
  
  log('RONG', 'INFO', 'UDP sending to %s:%s', 
    tostring(rong.conf.broadcast_to_ip or '255.255.255.255'), 
    tostring(rong.conf.protocol_port))
  
  --udp_out = assert(selector.new_udp(conf.broadcast_to_ip or '255.255.255.255', conf.protocol_port))
  udp_out = assert(selector.new_udp(nil, nil, rong.conf.listen_on_ip))
  for k, v in pairs(rong.conf.udp_opts) do
    log('RONG', 'INFO', 'UDP send opt %s : %s', tostring(k), tostring(v))
    if udp_out.fd.setoption then
      assert(udp_out.fd:setoption(k,(v==true or v==1)))
    elseif udp_out.fd.setopt then
      assert(udp_out.fd:setopt('socket', k, v))
    else
      error()
    end  
  end

  if udp_out.fd.setpeername then
    assert(udp_out.fd:setpeername(rong.conf.broadcast_to_ip or '255.255.255.255', rong.conf.protocol_port))
  elseif udp_out.fd.connect then
    assert(udp_out.fd:connect(rong.conf.broadcast_to_ip or '255.255.255.255', rong.conf.protocol_port))
  else
    error()
  end
  
end

M.broadcast = function ( m )
  --assert(udp_out:send_sync(m))
  udp_out.fd:send(m)
end

return M
