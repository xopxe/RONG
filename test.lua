--require "strict"
--look for packages one folder up.
package.path = package.path .. ";;;../?.lua;../?/init.lua"

local sched = require 'lumen.sched'
local log = require 'lumen.log'
log.setlevel('ALL', 'RONG')
local selector = require "lumen.tasks.selector"
selector.init({service='luasocket'})

local conf = {
  name = 'rongnode', --must be unique
  protocol_port = 8888,
  listen_on_ip = '*', 
  broadcast_to_ip = '127.255.255.255', --'164.73.36.255' --adress used when broadcasting
  udp_opts = {
    broadcast	= 1,
    dontroute	= 0,
  },
  send_views_timeout =  6, --5
}

local rong = require 'rong'.new(conf)
--rong:subscribe('sub1@'..conf.name, {{'node', '=', 'node1'}})


--[[  
local conf2 = {
  name = 'rongnode2', --must be unique
  protocol_port = 8888,
  listen_on_ip = '127.0.0.1', --'*', 
  broadcast_to_ip = '255.255.255.255', --'164.73.36.255' --adress used when broadcasting
  udp_opts = {
    broadcast	= 1,
    dontroute	= 0,
  },
  send_views_timeout =  60000, --5
}
local rong2 = require 'rong'.new(conf2)
rong2:subscribe('sub1@'..conf2.name, {{'node', '=', 'node2'}})
--]]  


local udp_out = assert(selector.new_udp(nil, nil, conf.listen_on_ip))
for k, v in pairs(conf.udp_opts) do
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

sched.run(function()
  local i=1
  while true do
    sched.sleep(5)
    local s='{"view":{"sub1@rongnode":{"1":["node","=","node1"],"visited":[],"seq":' .. i .. '}}}'
    print('OUT',s)
    udp_out:send(s)
    i=i+1
  end
end)
  
sched.loop()
