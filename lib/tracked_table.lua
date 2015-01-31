local M = {}

M.make_TrackedTable = function ()
	local function make_TrackedTable_MT ()
		local n = 0
		local MT={
			add=function(self, key, value)
				if not rawget(self, key) then
					n = n + 1
				end
				rawset(self, key, value)
			end,
			del=function(self, key)
				if rawget(self, key) and n > 0 then
					n = n - 1
				end
				rawset(self, key, nil)
			end,
			len=function(self)
				return n
			end
		}
		MT.__index=MT
		return MT
	end
	return setmetatable({},make_TrackedTable_MT())
end

return M

--[[
T=make_TrackedTable()
T:add('one',{})
T:add('one',{})
T:add('three',{})

--T:del('two')

for a,b in pairs(T) do
	print(a, b)
end
print (T:len())
--]]

