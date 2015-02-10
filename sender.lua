local socket = require 'socket'


local udp_out = assert(socket.udp())
assert (udp_out:setsockname('10.42.0.1', 0)) 
assert (udp_out:setoption('broadcast', true)) 
--assert (udp_out:setoption('dontroute', false))
assert (udp_out:setpeername('255.255.255.255', 8888))

local i=1
while true do
  local m='M '..i
  assert(udp_out:send(m))
  socket.sleep(1)
  i=i+1
end
