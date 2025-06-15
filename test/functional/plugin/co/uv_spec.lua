local t = require('test.testutil')
local n = require('test.functional.testnvim')()

local clear = n.clear
local eq = t.eq
local exec_lua = n.exec_lua

before_each(clear)

describe('co uv', function()
  it('spawn()', function()
    local exit_code, exit_signal, read_data = exec_lua(function()
      -- See :h uv.spawn()
      local stdin = assert(vim.uv.new_pipe())
      local stdout = assert(vim.uv.new_pipe())
      local stderr = assert(vim.uv.new_pipe())

      local handle, pid, cat_future = vim.co.uv.spawn('cat', { stdio = { stdin, stdout, stderr } })
      assert(handle and pid)

      local read_future = vim.co.future()
      vim.uv.read_start(stdout, function(err, data)
        assert(not err, err)
        if data ~= nil and not read_future.done then
          read_future:complete(data)
        end
      end)
      vim.uv.write(stdin, 'Hello World')

      local read_data
      local exit_code, exit_signal = vim.co
        .spawn(function()
          read_data = read_future:await()
          vim.co.uv.shutdown(stdin)
          return cat_future:await()
        end)
        :wait(200, 2)

      return exit_code, exit_signal, read_data
    end)

    eq('Hello World', read_data)
    eq(0, exit_code)
    eq(0, exit_signal)
  end)

  describe('shutdown', function()
    it('returns a fail on a non-writable pipe', function()
      local err, name = exec_lua(function()
        local p = assert(vim.uv.new_pipe())

        local err, name = vim.co
          .spawn(function()
            return vim.co.uv.shutdown(p)
          end)
          :wait(100, 1)

        return err, name
      end)

      eq('ENOTCONN', name)
      eq('ENOTCONN: socket is not connected', err)
    end)

    it('shuts down a pipe', function()
      local err_name = exec_lua(function()
        local stdin = assert(vim.uv.new_pipe())
        local handle = assert(vim.co.uv.spawn('cat', { stdio = { stdin } }))

        local _, err_name = vim.co
          .spawn(function()
            local shutdown_err = vim.co.uv.shutdown(stdin)
            return vim.co.uv.write(stdin, 'foo')
          end)
          :wait(100, 1)

        vim.uv.close(stdin)
        vim.uv.close(handle)

        return err_name
      end)

      eq('EPIPE', err_name)
    end)
  end)

  describe('write', function()
    it('returns a fail on a non-writable pipe', function()
      local err = exec_lua(function()
        local p = assert(vim.uv.new_pipe())
        local _, err_name = vim.co
          .spawn(function()
            return vim.co.uv.write(p, 'foo')
          end)
          :wait(100, 1)

        return err_name
      end)

      eq('EBADF', err)
    end)

    it('cancellation works', function()
      -- Only cares about no errors
      assert.has_no_error(function()
        exec_lua(function()
          local fds = vim.uv.pipe({ nonblock = true }, { nonblock = true })
          assert(fds ~= nil)
          local read_pipe = vim.uv.new_pipe()
          assert(read_pipe ~= nil)
          read_pipe:open(fds.read)
          local write_pipe = vim.uv.new_pipe()
          assert(write_pipe ~= nil)
          write_pipe:open(fds.write)

          vim.co
            .spawn(function()
              return vim.co.uv.write(write_pipe, 'foo')
            end)
            :cancel()

          if not vim.uv.is_closing(write_pipe) then
            vim.uv.close(write_pipe)
          end
          vim.uv.close(read_pipe)
        end)
      end)
    end)

    it('writes to a pipe', function()
      local result = exec_lua(function()
        local fds = assert(vim.uv.pipe({ nonblock = true }, { nonblock = true }))
        local read_pipe = assert(vim.uv.new_pipe())
        read_pipe:open(fds.read)
        local write_pipe = assert(vim.uv.new_pipe())
        write_pipe:open(fds.write)

        local read_future = vim.co.future()

        read_pipe:read_start(function(err, data)
          if read_future.done then
            error('This should not happen.')
          end
          if err then
            read_future:error(err)
          elseif data ~= nil then
            read_future:complete(data)
          end
        end)

        local result = vim.co
          .spawn(function()
            assert(nil == vim.co.uv.write(write_pipe, 'foo'))
            return read_future:await()
          end)
          :wait(100, 1)

        vim.uv.close(write_pipe)
        vim.uv.close(read_pipe)

        return result
      end)

      eq('foo', result)
    end)
  end)

  describe('fs_stat', function()
    after_each(function()
      os.remove('Xtest_fs_stat')
    end)
    it('returns file stats for an existing file', function()
      n.insert('\n')
      n.command('write Xtest_fs_stat')

      local has_size, ftype = exec_lua(function()
        local stat = vim.co
          .spawn(function()
            -- FIXME use a temp file
            local err, stat = vim.co.uv.fs_stat('Xtest_fs_stat')
            assert(not err)
            return stat
          end)
          :wait(10, 1)
        assert(stat)
        return stat.size > 0, stat.type
      end)

      eq(true, has_size)
      eq('file', ftype)
    end)

    it('returns an error for a non-existing file', function()
      local err = exec_lua(function()
        return vim.co
          .spawn(function()
            return vim.co.uv.fs_stat('Xtest_fs_stat')
          end)
          :wait(10, 1)
      end)

      assert.is_not_nil(err)
    end)
  end)

  describe('fs_read', function()
    after_each(function()
      os.remove('Xtest_fs_read')
    end)
    it('reads a file', function()
      local txt = '<!-- This is a test file -->\n'
      n.insert(txt)
      n.command('write Xtest_fs_read')
      local size = 10

      local header = exec_lua(function()
        return vim.co
          .spawn(function()
            local err_open, fd = vim.co.uv.fs_open('Xtest_fs_read', 'r', 0)
            assert(err_open == nil and fd)
            local err_read, data = vim.co.uv.fs_read(fd, size)
            vim.co.uv.fs_close(fd)
            return data
          end)
          :wait(100, 2)
      end)

      eq(txt:sub(1, size), header)
    end)
  end)
end)
