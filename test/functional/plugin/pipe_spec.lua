---@diagnostic disable: no-unknown
local t = require('test.testutil')
local n = require('test.functional.testnvim')()

local clear = n.clear
local eq = t.eq
local exec_lua = n.exec_lua

before_each(clear)

describe('vim.pipe', function()

  it('anext()', function()
    -- asynchronously waits for the next value in the pipe
    eq(
      { 2, 4, 6 },
      exec_lua([[
        local p = vim.pipe({ 3, 2, 1 }):map(function(x)
          vim.co.sleep(x)
          return x * 2
        end)
        return vim.co.spawn(function()
          local results = {}
          results[#results+1] = p:anext() -- 2
          results[#results+1] = p:anext() -- 4
          results[#results+1] = p:anext() -- 6
          results[#results+1] = p:anext() -- nil
          return results
        end):wait(100)
      ]])
    )
  end)

  it('next()', function()
    eq(
      { 1, 2, 3 },
      exec_lua([[
      local p = vim.pipe({ 1, 2, 3 })
      return {
        p:next(),
        p:next(),
        p:next(),
        p:next(),
      }
    ]])
    )
  end)

  it('new(tbl)', function()
    eq({ 1, 2, 3 }, exec_lua([[ return vim.pipe({ 1, 2, 3 }):totable() ]]))
  end)

  it('new({})', function()
    eq({}, exec_lua([[ return vim.pipe({}):totable() ]]))
  end)

  it('new(fn)', function()
    eq({ 'a', 'b', 'c' }, exec_lua([[ return vim.pipe(('abc'):gmatch('.')):totable() ]]))
  end)

  it('map(tf)', function()
    -- 'normal'
    eq(
      { 2, 4, 6 },
      exec_lua([[ return vim.pipe({ 1, 2, 3 }):map(function(x) return x * 2 end):totable() ]])
    )

    -- async
    eq(
      { 2, 4, 6 },
      exec_lua([[
        return vim.pipe({ 3, 2, 1 })
          :map(function(x)
            vim.co.sleep(x)
            return x * 2
          end)
          :totable()
      ]])
    )

    -- filters
    eq(
      { 2, 6 },
      exec_lua(
        [[ return vim.pipe({ 1, 2, 3 }):map(function(x) return x ~= 2 and x * 2 or nil end):totable() ]]
      )
    )
  end)

  it('map():next()', function()
    -- async
    eq(
      { 2, 4, 6 },
      exec_lua([[
        local p = vim.pipe({ 3, 2, 1 })
          :map(function(x)
            vim.co.sleep(x)
            return x * 2
          end)
        return {
          p:next(),
          p:next(),
          p:next(),
          p:next()
        }
      ]])
    )
  end)

  it('for loops', function()
    eq(
      { 1, 2, 3 },
      exec_lua([[
        local pipe = vim.pipe({1, 2, 3})
        local t = {}
        for v in pipe do
          table.insert(t, v)
        end
        return t
    ]])
    )

    eq(
      { 1, 2, 3 },
      exec_lua([[
        local pipe = vim.pipe({3, 2, 1}):map(function(x)
          vim.co.sleep(x)
          return x
        end)
        local t = {}
        for v in pipe do
          table.insert(t, v)
        end
        return t
    ]])
    )
  end)

  it('wait()', function()
    -- waits for the pipe to complete
    -- Does NOT return the results, pipe can still be modified / used
    eq(
      { true, { 2, 4, 6 } },
      exec_lua([[
      local p = vim.pipe({ 3, 2, 1 }):map(function(x)
        vim.co.sleep(x)
        return x * 2
      end)
      p:wait(50)
      return { p.task:is_done(), p:totable() }
    ]])
    )

    eq(
      false,
      exec_lua([[
      local p = vim.pipe({ 3, 2, 1 }):map(function(x)
        vim.co.sleep(x * 20)
        return x * 2
      end)
      return p.task:is_done()
    ]])
    )

  end)

  it('cancel()', function()
    -- cancels the pipe
    eq(
      true,
      exec_lua([[
        local p = vim.pipe({ 1, 2, 3 }):map(function(x)
          vim.co.sleep(x * 20)
          return x * 2
        end)
        p:cancel()
        return p.task:is_cancelled()
      ]])
    )
  end)

  it('each(tf)', function()
    eq(
      { 2, 4, 6 },
      exec_lua([[
        local t = {}
        vim.pipe({ 3, 2, 1 })
          :each(function(x)
            vim.co.sleep(x)
            table.insert(t, x * 2)
          end)
        return t
      ]])
    )
  end)

  it('filter(tf)', function()
    -- filters the results
    eq(
      { 2, 4 },
      exec_lua([[
        return vim.pipe({ 4, 3, 2, 1 })
          :filter(function(x)
            vim.co.sleep(x)
            return x % 2 == 0 -- Keep only even numbers
          end)
          :totable()
      ]])
    )
  end)

  it('collect(n)', function()
    -- returns the first n results, but does not stop pipe
    eq(
      { { 2, 4 }, { 6, 8 } },
      exec_lua([[
        local p = vim.pipe({ 4, 3, 2, 1 })
          :map(function(x)
            vim.co.sleep(x)
            return x * 2
          end)
        local t1 = { p:collect(2) } -- Take the first 2 values
        local t2 = { p:collect(4) } -- Take the rest, only 2 more
        return { t1, t2 }
      ]])
    )
  end)

  it('race(n)', function()
    -- returns the first n results, then cancels the pipe
    eq(
      { { 2, 4 }, true }, -- Fastest complete first
      exec_lua([[
        local p = vim.pipe({ 4, 3, 2, 1 })
          :map(function(x)
            vim.co.sleep(x)
            return x * 2
          end)
        return { { p:race(2) }, p.task:is_cancelled() }
      ]])
    )
  end)

  it('report()', function()
    eq(
      { { 2, 2 }, { '2', '3' } }, -- Error for x>1 is filtered out
      exec_lua([[
        local p = vim.pipe({ 1, 2, 3, 1 })
          :map(function(x)
            vim.co.sleep(x)
            if x > 1 then error(tostring(x), 0) end
            return x*2
          end)
        return { p:totable(), p:report() }
      ]])
    )
  end)

  describe('map() opts', function()
    -- task function opts
    it('throttle', function()
      eq(
        { 4, 20, 2 },
        exec_lua([[
          -- Time for each task + throttle: 10ms, 9ms, 12ms
          local tstart = vim.uv.now()
          local r = vim.pipe({ 10, 2, 1 })
            :map(function(x)
              vim.co.sleep(x)
              return x * 2
            end, { throttle = 5 })
            :totable()
            return r
        ]])
      )
    end)

    it('parallel', function()
      eq(
        { 6, 4, 2 },
        exec_lua([[
          return vim.pipe({ 3, 2, 1 })
            :map(function(x)
              vim.co.sleep(x)
              return x * 2
            end, { parallel = 1 })
            :totable()
        ]])
      )

      eq(
        { 4, 6, 2 },
        exec_lua([[
          return vim.pipe({ 3, 2, 1 })
            :map(function(x)
              vim.co.sleep(x)
              return x * 2
            end, { parallel = 2 })
            :totable()
        ]])
      )
    end)

    it('timeout', function()
      eq(
        { { 2, 2, 4 }, { 'timeout' } },
        exec_lua([[
          local p = vim.pipe({ 1, 3, 2, 1 })
            :map(function(x)
              vim.co.sleep(x * 2)
              return x * 2
            end, { timeout = 5 })
          return { p:totable(), p:report() }
        ]])
      )
    end)

    it('ordered sync', function()
      eq(
        { 6, 4, 2 },
        exec_lua([[
          return vim.pipe({ 3, 2, 1 })
            :map(function(x)
              return x * 2
            end, { ordered = true })
            :totable()
        ]])
      )
    end)

    it('ordered async', function()
      eq(
        { 6, 4, 2 },
        exec_lua([[
          return vim.pipe({ 3, 2, 1 })
            :map(function(x)
              vim.co.sleep(x)
              return x * 2
            end, { ordered = true }) -- runs async, returns in order
            :totable()
        ]])
      )
    end)

    it('ordered parallel work together', function()
      eq(
        { 6, 4, 2 },
        exec_lua([[
          return vim.pipe({ 3, 2, 1 })
            :map(function(x)
              vim.co.sleep(x)
              return x * 2
            end, { ordered = true, parallel = 2 }) -- runs async, returns in order
            :totable()
        ]])
      )
    end)

    it('catch', function()
      eq(
        { { 2, 2 }, { '2 is too big by 1', '3 is too big by 2' } },
        exec_lua([[
          local p = vim.pipe({ 1, 3, 2, 1 })
            :map(function(x)
              vim.co.sleep(x)
              if x > 1 then error(tostring(x), 0) end
              return x * 2
            end, { catch = function(err, n) error(err .. ' is too big by ' .. n - 1, 0) end})
          return { p:totable(), p:report() }
        ]])
      )
    end)

    it('filter catch', function()
      eq(
        { { 2, 2 } },
        exec_lua([[
          local p = vim.pipe({ 1, 3, 2, 1 })
            :map(function(x)
              vim.co.sleep(x)
              if x > 1 then error(tostring(x), 0) end
              return x * 2
            end, { catch = function() end})
          return { p:totable(), p:report() }
        ]])
      )
    end)

  end)

  it('all(tf)', function()
    eq(
      { true, false },
      exec_lua([[
        local gt0 = function(x)
          vim.co.sleep(x)
          return x > 0
        end
        return { vim.pipe({ 2, 1 }):all(gt0), vim.pipe({ 1, 0 }):all(gt0) }
      ]])
    )
  end)

  it('any(tf)', function()
    eq(
      { true, false },
      exec_lua([[
      local has_2 = function(x)
        vim.co.sleep(x)
        return x == 2
      end
      return {
        vim.pipe({ 1, 4, 2, 3 }):any(has_2),
        vim.pipe({ 1, 4, 3, 3 }):any(has_2),
      }
    ]])
    )

    eq(
      false,
      exec_lua([[ return vim.pipe({ 1, 2, 3 }):any(function(x) return x < 1 end) ]])
    )
  end)

  it('await()', function()
    -- async waits for the pipe to complete (in a task)
    eq(
      { 2, 4, 6 },
      exec_lua([[
        return vim.co.spawn(function()
          local p = vim.pipe({ 3, 2, 1 })
            :map(function(x)
              vim.co.sleep(x)
              return x * 2
            end)
          p:await() -- await inside a task
          return p:totable()
        end):wait(100)
      ]])
    )
  end)

  it('fold(fn)', function()
    -- folds the results into a single value, waits for async stages
    eq(
      12, -- 6 + 4 + 2
      exec_lua([[
        return vim.pipe({ 1, 2, 3 })
          :map(function(x)
            vim.co.sleep(x)
            return x * 2
          end)
          :fold(0, function(acc, val)
            return acc + val
          end)
          -- Fold implicitly waits
      ]])
    )
  end)

end)
