--- @brief Asynchronous iterator pipelines.
---
--- [vim.pipe()]() is an interface for asynchronous iterables. It wraps a
--- table or function iterator into a [Pipe]() object. This object has methods
--- (like [Pipe:map()] and [Pipe:filter()]) that transform the data asynchronously.
---
--- These methods can be chained to create data processing "pipelines", where
--- each stage runs as a [vim.co]() task.

---@diagnostic disable: no-unknown, unused-local

local co = vim.co

--- LuaLS is bad at generics which this module mostly deals with
--- @diagnostic disable:no-unknown

--- @diagnostic disable:unused-function

-- methods that take a task function (tf) also can take opts:
-- { timeout: number, jobs: number, throttle: number, ordered: boolean }

-- report() should say something about when the error occurs? and what args

local packedmt = {}

local function unpack(t, ...)
  if select('#', ...) == 0 and getmetatable(t) == packedmt then
    return _G.unpack(t, 1, t.n)
  end
  return _G.unpack(t, ...)
end

local function pack(...)
  return setmetatable({ n = select('#', ...), ... }, packedmt)
end

local function sanitize(t)
  if type(t) == 'table' and getmetatable(t) == packedmt then
    -- Remove length tag and metatable
    t.n = nil
    setmetatable(t, nil)
  end
  return t
end

---@alias Awaitable Task | Future

--- Waits for any of the given awaitables to complete.
---
---@async
---@param aws Awaitable[]
---@return Awaitable done the first awaitable that completed
---@return Awaitable[] done the remaining awaitables
local function await_any(aws)
  if #aws == 0 then
    error('The list of awaitables is empty.', 0)
  end
  local future = co.future()

  for i, aw in ipairs(aws) do
    aw:await(function()
      if future.done then
        -- There’s already an awaitable that has completed.
        return
      end
      future:complete(i, aw)
    end)
  end

  local aw_pos, aw = future()
  table.remove(aws, aw_pos)
  return aw, aws
end

---@async
---@param aws Awaitable[]
---@return table results The results of the awaitables.
local function await_all(aws)
  local done_count = 0
  local this = co.running()
  if this == nil then
    error('await_all can only be used in a task.')
  end
  local results = {}
  for _ = 1, #aws do
    table.insert(results, nil)
  end

  for i, f in ipairs(aws) do
    f:await(function(...)
      results[i] = { ... }
      done_count = done_count + 1
      if done_count == #aws and co.status(this) == 'suspended' then
        co.resume(this)
      end
    end)
  end

  if done_count < #aws then
    co.yield()
  end
  return results
end


-- multi consumer multi producer queue
---@private
---@class Queue
---@field waiting? List<Task>
---@field values? List<any>
local Queue = {}

---@private
---@class Semaphore
---@field waiting? List<Task>
local Semaphore = {}

local DONE = {} -- sentinel value for when the queue is done

do -- define Queue and Semaphore
  ---@private
  ---@class List
  ---@field first integer
  ---@field last integer
  local List = {}

  function List.new()
    return setmetatable({ first = 1, last = 0 }, { __index = List })
  end

  function List:push(value)
    local last = self.last + 1
    self.last = last
    self[last] = value
  end

  function List:pop()
    if self:empty() then
      error('list is empty')
    end
    local value = self[self.first]
    self[self.first] = nil
    self.first = self.first + 1
    return value
  end

  function List:empty()
    return self.first > self.last
  end

  ---@return Queue
  Queue.new = function()
    return setmetatable({
      waiting = List.new(),
      values = List.new(),
    }, { __index = Queue })
  end

  function Queue:push(...)
    if self.waiting:empty() then
      self.values:push(...)
      return
    end
    local task = self.waiting:pop()
    task:resume(...)
  end

  function Queue:pop()
    if not self.values:empty() then
      return self.values:pop()
    end

    local this = co.running()
    if this == nil then
      error('Pop must be called within a task.')
    end

    self.waiting:push(this)
    local running, value = co.pyield()
    if running then
      return value
    else
      error(value, 0)
    end
  end

  function Queue:empty()
    return self.values:empty()
  end

  function Semaphore.new(count)
    return setmetatable({
      count = count,
      waiting = List.new(),
    }, { __index = Semaphore })
  end

  function Semaphore:acquire()
    if self.count > 0 then
      self.count = self.count - 1
      return true
    end

    local this = co.running()
    if this == nil then
      error('acquire must be called within a task.')
    end

    self.waiting:push(this)
    local running = co.pyield()
    if running then
      return true
    else
      error(running, 0)
    end
  end

  function Semaphore:release()
    if not self.waiting:empty() then
      local task = self.waiting:pop()
      task:resume(true)
    else
      self.count = self.count + 1
    end
  end

end

---@class Pipe
---@field private task Task
---@field private _errors table
local Pipe = {}
Pipe.__index = Pipe

--- Creates a task function that times out after the given duration.
---
--- If no timeout is taking place, `timeout(duration, tf, ...)` is equivalent to `tf(...)`.
---
--- If a timeout happens, `timeout` throws `error("timeout")`.
---
--- If the returned task function is cancelled, so is the wrapped task function.
---
---@async
---@param duration integer The duration in milliseconds.
---@param tf async function The task function to run.
---@param ... ... The arguments to pass to the task function.
---@return ... The results of the task function.
local function util_timeout(duration, tf, ...)
  local t = co.spawn(tf, ...)
  local timed_out = false

  -- Start the watchdog.
  co.spawn(function()
    co.sleep(duration)
    if t:status() ~= 'dead' then
      timed_out = true
      t:cancel()
    end
  end)

  local results = pack(t:pawait())

  if t:status() == 'dead' then
    if results[1] then
      return unpack(results, 2, results.n)
    elseif timed_out == true then
      error('timeout', 0)
    else
      error(results[2], 0)
    end
  else
    assert(
      co.running():is_cancelled(),
      'timeout encountered an invalid state. Report to Coop.nvim maintainer.'
    )
    t:cancel()
    error(results[2], 0)
  end
end

--- Creates a new Pipe object from the given |iterable|.
---
---@param src table|function Table or iterator to drain values from
---@return Pipe
---@private
function Pipe.new(src, ...)
  local iter = vim.iter(src, ...)
  return setmetatable({
    anext = function()
      return iter:next()
    end,
  }, Pipe)
end

--- Returns the next item from the pipe, blocking until it is available.
function Pipe:next(timeout, interval)
  timeout = timeout or 100000
  return co.spawn(self.anext, self):wait(timeout, interval)
end

--- Returns the next item from the pipe, only usable in a task.
function Pipe:anext()
  -- This function is provided by the source iterator in Pipe.new.
  -- This definition exists only for the docstring
end

--- Waits for the final stage of the pipe to complete, but does not close the pipe.
---@see |Task:wait()| |vim.wait()|
function Pipe:wait(...)
  if self.task then
    self.task:wait(...)
  end
end

--- Awaits for the final stage of the pipe to complete, but does not close the pipe.
---@param cb? fun(...):... Callback function to call when the pipe is done until this point.
---@see |Task:await()|
function Pipe:await(cb)
  if self.task then
    self.task:await(cb)
  end
end

--- Cancels the pipe and all tasks in it.
function Pipe:cancel()
  if self.task then
    self.task:cancel()
  end
end

--- Optional keyword arguments:
--- @class StageOpts
--- @inlinedoc
---
--- Timeout in milliseconds for each task (default: nil, no timeout)
--- @field timeout integer?
---
--- Maximum number of tasks to run in parallel (default: nil, unlimited)
--- @field parallel integer?
---
--- Milliseconds to wait between starting tasks (default: 0)
--- @field throttle integer?
---
--- If true, results are yielded in the original order (default: false)
--- @field ordered boolean?
---
--- A function to catch errors in the stage. If the function returns
--- true, ..., the stage will attempt to try again with the results.
--- @field catch fun(err, ...):...

--- Maps the items of the pipe to the values returned by `tf`.
---
--- Each item from the previous stage is passed to `func`. The `func` can be
--- a `vim.co` task function (i.e., it can use `vim.co.sleep`, etc.).
--- If `func` returns `nil`, the item is filtered out.
---
---@param tf fun(...):...:any The mapping task function. Takes all values from 
---                       returned from the previous stage in the pipeline as 
---                       arguments and returns one or more new values, which 
---                       are used in the next pipeline stage. Nil return
---                       values are filtered from the output.
---@param opts? StageOpts Options for the stage.
---@return Pipe
function Pipe:map(tf, opts)
  vim.validate('tf', tf, 'function')
  vim.validate('opts', opts, 'table', true)
  if opts then
    vim.validate('opts.timeout', opts.timeout, 'number', true)
    vim.validate('opts.parallel', opts.parallel, 'number', true)
    vim.validate('opts.throttle', opts.throttle, 'number', true)
    vim.validate('opts.ordered', opts.ordered, 'boolean', true)
    vim.validate('opts.catch', opts.catch, 'function', true)
  end

  opts = opts or {}
  local anext = self.anext --[[@as fun(): ...]]

  local output_queue = Queue.new()

  local handler_tasks = {}
  local worker = Semaphore.new(opts.parallel or math.huge)
  local next_start = vim.uv.now()

  -- Handler for the tf that handles options and output
  local function tf_handler(handle_index, ...)

    worker:acquire()
    -- If throttle is set, wait until the next start time
    if opts.throttle then
      local dt = next_start - vim.uv.now()
      next_start = next_start + opts.throttle
      if dt > 0 then
        co.sleep(dt)
      end
    end
    -- set up the task and timeout
    local task = co.create(tf)

    local timed_out = false
    if opts.timeout then
      co.spawn(function()
        co.sleep(opts.timeout)
        if task:status() ~= 'dead' then
          timed_out = true
          task:cancel()
        end
      end)
    end

    -- start doing the work
    co.resume(task, ...)
    local result = pack(task:pawait())

    -- catch errors if specified
    if opts.catch and not result[1] then
      result = pack(co.pcall(opts.catch, result[2], ...))
    end

    worker:release()

    if timed_out then
      result = { false, 'timeout' }
    end

    if not result[1] then
      if not self._errors then
        self._errors = {}
      end
      table.insert(self._errors, result[2])
    elseif result[2] ~= nil then -- filters nils from results
      if opts.ordered and handle_index > 1 then
        -- only queue the result after the prev. worker is done.
        handler_tasks[handle_index - 1]:await()
      end
      output_queue:push(pack(unpack(result, 2, result.n))) -- FIXME: ew
    end
  end

  -- The runner task manages spawning workers and feeding the output queue.
  self.task = co.spawn(function()
    local this = co.running()

    local item = pack(anext())
    while item[1] ~= nil do
      table.insert(handler_tasks, co.spawn(tf_handler, #handler_tasks + 1, unpack(item)))
      item = pack(anext())
    end

    -- all tasks are spawned, when they are completed we can signal that it's done
    for _, task in ipairs(handler_tasks) do
      task:await()
    end
    output_queue:push(DONE)
  end)

  self.anext = function()
    if self.task:is_cancelled() then
      error('Pipe was cancelled.', 0)
    end

    local result = output_queue:pop()

    if result == DONE then
      output_queue:push(DONE)
      return nil
    end

    return unpack(result)
  end

  return self
end

--- Filters an iterator pipeline.
---
---@param tf fun(...):boolean The filter task function. Takes all values
---                   returned from the previous stage in the pipeline and
---                   returns false or nil if the current element should be
---                   removed.
---@param opts? StageOpts Options for the stage.
---@return Pipe
function Pipe:filter(tf, opts)
  return self:map(function(...)
    if tf(...) then
      return ...
    end
  end, opts)
end

--- Returns true if all items in the pipe match the given predicate.
---
---@param tf_pred fun(...):boolean The predicate task function. Takes all values
---                        returned from the previous stage in the pipeline as
---                        arguments and returns true if the predicate matches.
---@return boolean
function Pipe:all(tf_pred, opts)
  local all = true
  self:map(function(...)
    if not tf_pred(...) then
      all = false
    end
    return all
  end, opts)

  while self:next() do
  end
  return all
end

--- Returns true if any item in the pipe matches the given predicate.
--- Cancels the pipe after the first match.
---
---@param tf_pred fun(...):boolean The predicate task function. Takes all values
---                        returned from the previous stage in the pipeline as
---                        arguments and returns true if the predicate matches.
---@return boolean
function Pipe:any(tf_pred, opts)
  local any = false

  self:map(function(...)
    if tf_pred(...) then
      any = true
    else
      return true
    end
  end)

  while self:next() do
  end
  self:cancel()
  return any
end

--- Folds ("reduces") a pipe into a single value.
---
---
---@generic A
---
---@param init A Initial value of the accumulator.
---@param f fun(acc:A, ...):A Accumulation function.
---@return A
---@see |Iter:fold()|
function Pipe:fold(init, fn)
  return vim.iter(self):fold(init, fn)
end

--- Calls the given task function for each item in the pipe, draining the pipe.
---
--- For functions with side effects. To modify the values in the iterator, use |Pipe:map()|.
---
---@param tf fun(...) Function to execute for each item in the pipeline.
---                   Takes all of the values returned by the previous stage
---                   in the pipeline as arguments.
function Pipe:each(tf, opts)
  self:map(tf, opts)
  while self:next() do
  end
end

--- Returns the next `n` items from the pipe, blocking until they are available.
function Pipe:collect(n)
  if n == 1 then
    return self:next()
  end
  return self:next(), self:collect(n - 1)
end

--- Returns the first `n` items from the pipe, then cancels the pipe
function Pipe:race(n)
  local res = { self:collect(n) }
  self:cancel()
  return unpack(res)
end

--- Returns errors that occurred in the pipe, if any
function Pipe:report(n)
  if self._errors then
    return self._errors
  end
end

--- Collects the pipe into a table.
function Pipe:totable()
  local t = {}
  local i = 0
  repeat
    i = i + 1
    table.insert(t, sanitize(self:next()))
  until t[i] == nil
  return t
end

Pipe.__call = Pipe.next

-- Do these make sense to have?
-- enumerate, nth, slice, skip, peek
-- I don't think these do:
-- rev, find, rfind, rpeek, rskip, pop, join
-- vim.validate all the things

---@nodoc
---@class PipeMod
---@operator call:Pipe

local M = {}

return setmetatable(M, {
  __call = function(_, ...)
    return Pipe.new(...)
  end,
}) --[[@as PipeMod]]
