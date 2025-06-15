---@diagnostic disable: no-unknown
local t = require('test.testutil')
local n = require('test.functional.testnvim')()
local eq = t.eq

-- TODO: pcall_err probably

local clear = n.clear
local exec_lua = n.exec_lua
local insert = n.insert

before_each(clear)

describe('vim.co', function()
  describe('future()', function()
    it('future await calls callback immediately for completed future', function()
      eq({ true, 1, 2 }, {
        exec_lua(function()
          local future = vim.co.future()
          future:complete(1, 2)

          local success, r1, r2 = false, nil, nil
          future:await(function(...)
            success, r1, r2 = ...
          end)
          return success, r1, r2
        end),
      })
    end)

    it('co.pcall await returns errors like pcall for async errors', function()
      eq({ false, 'foo' }, {
        exec_lua(function()
          local future = vim.co.future()
          local success, err = true, ''
          vim.co.spawn(function()
            success, err = vim.co.pcall(future.await, future)
          end)
          future:error('foo')
          return success, err
        end),
      })
    end)

    it('wait throws an error if the future throws an error', function()
      eq({ false, 'foo' }, {
        exec_lua(function()
          return pcall(function()
            vim.co
              .spawn(function()
                error('foo', 0)
                return 'foo'
              end)
              :wait(10)
          end)
        end),
      })
    end)

    it('await returns nothing if the future is still unfinished', function()
      eq({ nil }, {
        exec_lua(function()
          local task = vim.co.spawn(function()
            vim.co.sleep(1000)
            return 'foo'
          end)
          return task:wait(1)
        end),
      })
    end)

    it('pawait returns error when cancelled', function()
      eq({ true, false, 'cancelled' }, {
        exec_lua(function()
          local future = vim.co.future()
          local task = vim.co.spawn(function()
            return future:pawait()
          end)
          return task:cancel()
        end),
      })
    end)

    it('pawait still works when cancelled but the awaitable continues', function()
      exec_lua(function()
        local future = vim.co.future()
        vim.co.spawn(future.pawait, future):cancel()
        future:complete('foo')
      end)
    end)

    it('pawait throws error if called outside a task', function()
      eq({ false, 'Future.pawait can only be used in a task.' }, {
        exec_lua(function()
          local future = vim.co.future()
          return pcall(future.pawait, future)
        end),
      })
    end)
  end)

  describe('sleep()', function()
    it('sleeps for some time in an asynchronous coroutine', function()
      eq({ false, true }, {
        exec_lua(function()
          local done = false
          local task = vim.co.spawn(function()
            vim.co.sleep(15)
            done = true
          end)
          local before = done
          task:wait(20, 2)
          return before, done
        end),
      })
    end)

    it('works with an vim.api call', function()
      insert('foo')
      eq({ 'foo' }, {
        exec_lua([[
        return vim.co
          .spawn(function()
            vim.co.sleep(5)
            vim.co.sleep_until_nonfast()
            return vim.api.nvim_buf_get_lines(0, 0, 1, false)[1]
          end)
          :wait(100, 20)
      ]]),
      })
    end)

    it('handles cancellation', function()
      eq({ false, 'cancelled', false }, {
        exec_lua(function()
          local done = false
          local task = vim.co.spawn(function()
            vim.co.sleep(15)
            done = true
          end)
          task:cancel()
          local success, result = pcall(task.wait, task, 20, 2) -- Pass task as self
          return success, result, done
        end),
      })
    end)
  end)

  it('spawn()', function()
    eq({ false, true }, {
      exec_lua(function()
        local task = vim.co.spawn(function()
          vim.co.sleep(5)
        end)
        local before = task:is_done()
        vim.wait(30, function()
          return task:is_done()
        end)
        return before, task:is_done()
      end),
    })
  end)

  describe('cancel()', function()
    it('kills thread and marked cancelled', function()
      eq({ false, false, 'cancelled', 'dead', true }, {
        exec_lua(function()
          local done = false
          local task = vim.co.spawn(function()
            vim.co.sleep(5)
            done = true
          end)
          task:cancel()
          local success, result = pcall(task)
          -- Wait to see if the task sets the done flag. It shouldn’t.
          vim.wait(10)
          return done, success, result, task:status(), task:is_cancelled()
        end),
      })
    end)

    it('can be captured in yield', function()
      eq({ false, 'cancelled' }, {
        exec_lua(function()
          local success, err_msg = true, ''
          local task = vim.co.spawn(function()
            success, err_msg = vim.co.pcall(vim.co.sleep, 200)
            error(err_msg)
          end)

          -- Immediately cancel the task.
          task:cancel()
          return success, err_msg
        end),
      })
    end)

    it('can be uncancelled by own thread', function()
      eq({ false, 'done' }, {
        exec_lua(function()
          local task = vim.co.spawn(function()
            local success = vim.co.pcall(vim.co.sleep, 20)
            if not success and vim.co.running():is_cancelled() then
              vim.co.running():unset_cancelled()
            end
            return 'done'
          end)

          -- Immediately cancel the task.
          task:cancel()

          local result = task:wait(50, 10)
          return task:is_cancelled(), result
        end),
      })
    end)

    it('doesn’t do anything on finished tasks', function()
      eq({ false }, {
        exec_lua(function()
          local task = vim.co.spawn(function() end)
          task:wait(5, 1)
          task:cancel()
          return task:is_cancelled()
        end),
      })
    end)

    it('causes yield to throw an error after interception', function()
      eq({ false, 'cancelled', true, true }, {
        exec_lua(function()
          local running, err_msg = true, ''
          local task = vim.co.spawn(function()
            running, err_msg = vim.co.pyield()

            local _, msg = pcall(vim.co.pyield)
            return msg
          end)
          local success, result = task:cancel()
          return running,
            err_msg,
            success,
            vim.startswith(result, 'Called pyield inside a cancelled task.')
        end),
      })
    end)

    it('throws an error if called within a running task', function()
      eq({ false, 'You cannot cancel the currently running task.' }, {
        exec_lua(function()
          local success, err = true, ''
          vim.co.spawn(function()
            ---@diagnostic disable-next-line: cast-local-type
            success, err = pcall(vim.co.cancel, vim.co.running())
          end)
          return success, err
        end),
      })
    end)

    it('cancels subtasks', function()
      eq({ true }, {
        exec_lua(function()
          local subtask
          local task = vim.co.spawn(function()
            subtask = vim.co.spawn(function()
              vim.co.sleep(1000)
            end)
            vim.co.yield()
          end)
          task:cancel()
          return subtask:is_cancelled()
        end),
      })
    end)

    it('orphans subtasks', function()
      eq({ false }, {
        exec_lua(function()
          local subtask
          local task = vim.co.spawn(function()
            subtask = vim.co.spawn(function()
              vim.co.sleep(1000)
            end)
            vim.co.yield()
          end)
          task:cancel(true)
          return subtask:is_cancelled()
        end),
      })
    end)
  end)

  describe('resume()', function()
    it('throws an error on dead tasks', function()
      eq({ false, 'Tried to resume a task that is not suspended but dead.' }, {
        exec_lua(function()
          local task = vim.co.spawn(function() end)
          task:wait(5, 1)
          local success, err_msg = pcall(vim.co.resume, task)
          return success, err_msg
        end),
      })
    end)

    it('captures the final return value', function()
      eq({ true, 'foo' }, {
        exec_lua(function()
          local task = vim.co.create(function()
            return 'foo'
          end)
          return task:resume()
        end),
      })
    end)
  end)

  describe('yield()', function()
    it('throws if used outside of a task', function()
      eq(
        { false, 'Called pyield outside of a running task. Make sure that you use yield in tasks.' },
        {
          exec_lua(function()
            local task = vim.co.create(function()
              vim.co.yield()
            end)
            return coroutine.resume(task.thread) -- FIXME why coroutine (coop)?
          end),
        }
      )
    end)

    it('throws if resumed outside of a task', function()
      eq({
        false,
        'coroutine.yield returned without a running task. Make sure that you use task.resume to resume tasks.',
      }, {
        exec_lua(function()
          local task = vim.co.create(function()
            vim.co.yield()
          end)
          task:resume()
          return coroutine.resume(task.thread)
        end),
      })
    end)

    it('throws if cancelled', function()
      eq({ false, 'cancelled' }, {
        exec_lua(function()
          local task = vim.co.spawn(function()
            vim.co.yield()
          end)
          return task:cancel()
        end),
      })
    end)
  end)

  it('pawait() returns errors', function()
    eq({ false, 'foo' }, {
      exec_lua(function()
        local task = vim.co.spawn(function()
          vim.co.sleep(5)
          error('foo', 0)
        end)
        local s, e = vim.co
          .spawn(function()
            return task:pawait() -- only allowed within a task
          end)
          :wait(20, 1)
        return s, e
      end),
    })
  end)

  it('works in fast events', function()
    exec_lua([[
      vim.uv.new_timer():start(1, 0, function()
        assert(vim.in_fast_event())
        local task = vim.co.spawn(function()
          vim.co.sleep(1)
          assert(vim.in_fast_event())
          return 1
        end)
        local res = task:wait(10, 1)
        assert(res == 1)
      end)
    ]])
  end)
end)
