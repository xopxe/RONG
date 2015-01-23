local M = {}

local log = require 'lumen.log'
local selector = require 'lumen.tasks.selector'

M.new = function (rong)
  local udp_in, udp_out
  local conf = assert(rong.conf)

  local last_bcast --for filtering out own broadcast receives

  local net = {}
  
  net.build_socket = function ( _, handler )
    log('RONG', 'INFO', 'UDP listening on %s:%s', 
      tostring(conf.listen_on_ip), 
      tostring(conf.protocol_port))
    
    local handler_wrap = function(sktd, data, err, part)
      if data == last_bcast then
        return true
      else
        return handler(sktd, data, err, part)
      end 
    end
    
    
    udp_in = assert( selector.new_udp(nil, nil, 
        conf.listen_on_ip, conf.protocol_port, -1, handler_wrap))
    
    log('RONG', 'INFO', 'UDP sending to %s:%s', 
      tostring(conf.broadcast_to_ip or '255.255.255.255'), 
      tostring(conf.protocol_port))
    
    udp_out = assert(selector.new_udp(nil, nil, conf.listen_on_ip))
    
    for k, v in pairs(conf.udp_opts) do
      log('RONG', 'INFO', 'UDP send opt %s : %s', 
        tostring(k), tostring(v))
      if udp_out.fd.setoption then
        assert(udp_out.fd:setoption(k,(v==true or v==1)))
      elseif udp_out.fd.setopt then
        assert(udp_out.fd:setopt('socket', k, v))
      else
        error()
      end  
    end

    if udp_out.fd.setpeername then
      assert(udp_out.fd:setpeername(
          conf.broadcast_to_ip or '255.255.255.255',
          conf.protocol_port))
    elseif udp_out.fd.connect then
      assert(udp_out.fd:connect(
        conf.broadcast_to_ip or '255.255.255.255', 
        conf.protocol_port))
    else
      error()
    end
    
  end

  net.broadcast = function ( _, m )
    --assert(udp_out:send_sync(m))
    last_bcast = m
    udp_out.fd:send(m)
  end
    
  return net
end

--local udp = selector.new_udp(conf.udp.broadcast or '255.255.255.255', 
--conf.port, conf.ip, conf.port, -1, messages.incomming_handler)

return M
