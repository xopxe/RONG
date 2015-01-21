--require "strict"
--look for packages one folder up.
package.path = package.path .. ";;;../?.lua;../?/init.lua"

local sched = require 'lumen.sched'
local log = require 'lumen.log'
log.setlevel('ALL', 'RONG')
require "lumen.tasks.selector".init({service='luasocket'})

local conf = {
  name = 'rong2node', --must be unique
  protocol_port = 8888,
  listen_on_ip = '127.0.0.1', --'*', 
  broadcast_to_ip = '255.255.255.255', --'164.73.36.255' --adress used when broadcasting
  udp_opts = {
    broadcast	= 1,
    dontroute	= 0,
  },
  send_views_timeout =  6, --5
}

local rong = require 'rong'.new(conf)
rong:subscribe('sub1@'..conf.name, {{'node', '=', 'node1'}})
  
sched.loop()
