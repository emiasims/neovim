--- @brief
---
--- [vim.co] is an interface for working with coroutines in Neovim. It wraps
--- task functions to control the execution of coroutines and provides controls
--- to handle task cancellation and futures.
---
--- Task functions are functions that may call [vim.co.yield()] to suspend the
--- task. They must not call [coroutine.yield()] directly.
---
--- [Task]() objects are lua tables that contain the information about the task.
--- [Future]() objects are lua tables that contain the information about the promise.

local M = vim._defer_require('vim.co', {
  uv = ..., --- @module 'vim.co.uv'
})

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

-- TODO: documentation (inline opts too)
-- vim.validation

---@type Task?
local running_task

--- A table of tasks and their parents. Can be used to cancel task trees
---@type table<Task, table<Task, boolean>>
local Tasks = setmetatable({}, { __mode = 'k' })

--- Spawns a task function in a thread.
---
--- The returned task can be turned back into a task function.
--- spawn(f_co, ...)() is semantically the same as f_co(...)
---
---@param f_co function The task function to spawn.
---@return Task task the spawned task
function M.spawn(f_co, ...)
  local spawned_task = M.create(f_co)
  M.resume(spawned_task, ...)
  return spawned_task
end

--- Executes a coroutine or task function in a protected mode.
---
--- co.pcall is a coroutine alternative to pcall. pcall is not suitable for coroutines, because
--- yields can’t cross a pcall. co.pcall goes around that restriction by using coroutine.resume.
---
---
---@async
---@param f_co async function A coroutine function to execute.
---@return boolean success Whether the coroutine function executed successfully.
---@return ... The results of the coroutine function.
function M.pcall(f_co, ...)
  local thread = coroutine.create(f_co)
  local results = pack(coroutine.resume(thread, ...))
  while true do
    if not results[1] then
      return false, results[2]
    end

    if coroutine.status(thread) == 'dead' then
      return true, unpack(results, 2, results.n)
    end

    local args = pack(coroutine.yield())
    results = pack(coroutine.resume(thread, unpack(args)))
  end
end

--- Sleeps for a number of milliseconds.
---
---@async
---@param ms number The number of milliseconds to sleep.
function M.sleep(ms)
  local timer = assert(vim.uv.new_timer())
  local success, err = M.pcall(M.uv.timer_start, timer, ms, 0)
  -- Safely close resources even in case of a cancellation error.
  timer:stop()
  timer:close()
  if not success then
    error(err, 0)
  end
end

--- Resumes a task with the specified arguments.
---
---@param task Task the task to resume
---@param ... ... the arguments
---@return boolean success
---@return any ... results
function M.resume(task, ...)
  if task:status() ~= 'suspended' then
    error('Tried to resume a task that is not suspended but ' .. task:status() .. '.', 0)
  end

  local previous_task = M.running()
  running_task = task
  local results = pack(coroutine.resume(task.thread, ...))
  running_task = previous_task

  if not results[1] then
    task.future:error(results[2])
  elseif coroutine.status(task.thread) == 'dead' then
    task.future:complete(unpack(results, 2, results.n))
  end

  return unpack(results)
end

--- Cancels a task.
---
--- The cancelled task will throw `error("cancelled")` in its yield.
--- If you intercept cancellation, you need to unset the `cancelled` flag with with
--- Task:unset_cancelled.
---
--- `cancel` resumes the task. It’s like sending a cancellation signal that the task needs to
--- handle.
---
---@param task Task the task to cancel
---@param orphan? boolean If true, the task will not cancel its subtasks.
---@return boolean success
---@return any ... results
function M.cancel(task, orphan)
  local task_status = M.status(task)

  if task_status == 'running' or task_status == 'normal' then
    error('You cannot cancel the currently running task.', 0)
  end

  ---@type { [1]: boolean, [number]: any }
  local results
  if task_status == 'dead' then
    results = { false, 'dead' }
  else -- suspended
    task.cancelled = true
    results = { task:resume() }
  end

  if not orphan then
    for subtask, _ in pairs(Tasks[task] or {}) do
      table.insert(results, { M.cancel(subtask, orphan) })
    end
  end

  return _G.unpack(results)
end

--- Returns the currently running task or nil.
---
---@return Task?
function M.running()
  return running_task
end

function M.sleep_until_nonfast(...)
  if not vim.in_fast_event() then
    return
  end

  local task = assert(M.running())
  vim.schedule(function()
    task:resume()
  end)
  return M.yield()
end

function M.in_main()
  return not (vim.in_fast_event() or coroutine.running())
end

--- Yields the current task.
---
--- If inside a cancelled task, it throws an error message "cancelled".
---
---@async
---@param ... ... the arguments
---@return any ... the arguments passed to task.resume
function M.yield(...)
  local res = pack(M.pyield(...))

  if res[1] == true then
    return unpack(res, 2, res.n)
  else
    error(res[2], 0)
  end
end

--- Yields the current task.
---
--- `p` stands for "protected" like in `pcall`.
---
---@async
---@param ... ... the arguments
---@return boolean success true iff the task was not cancelled
---@return any ... the arguments passed to task.resume or "cancelled"
function M.pyield(...)
  local this = M.running()

  if not this then
    error('Called pyield outside of a running task. Make sure that you use yield in tasks.', 0)
  end

  if this:is_cancelled() then
    error(
      'Called pyield inside a cancelled task.'
        .. ' If you want to intercept cancellation,'
        .. ' you need to clear the cancellation flag with unset_cancelled.',
      0
    )
  end

  local args = pack(coroutine.yield(...))

  this = M.running()

  if not this then
    error(
      'coroutine.yield returned without a running task. Make sure that you use task.resume to resume tasks.',
      0
    )
  end

  if this.cancelled then
    return false, 'cancelled'
  end

  return true, unpack(args)
end

--- Returns the status of a task’s thread.
---
---@param task Task the task
---@return "running" | "suspended" | "normal" | "dead"
function M.status(task)
  return coroutine.status(task.thread)
end

--- Returns true if the task is done.
---
---@param task Task the task
---@return boolean
function M.is_done(task)
  return task.future.done
end

do -- define Task and Future
  --- A future is a synchronization mechanism that allows waiting for a task to return.
  ---
  --- Futures turn spawned task functions back into task functions as they implement the call operator.
  ---
  ---@class Future
  ---@field done boolean Whether the future is done.
  ---@field results table The results of the coroutine in pcall/coroutine.resume + pack format.
  ---@field queue table The queue of callbacks to be called once the future is done.
  local Future = {}
  Future.__index = Future

  --- Marks the future as done with the specified results and calls callbacks in the waiting queue.
  ---
  ---@param ... any The results of the coroutine function.
  function Future:complete(...)
    if self.done then
      error('Tried to complete an already done future.')
    end

    self.results = pack(true, ...)
    self.done = true
    for _, cb in ipairs(self.queue) do
      cb(unpack(self.results))
    end
    self.queue = {}
  end

  --- Marks the future as done with an error and calls callbacks in the waiting queue.
  ---
  ---@param self Future the future
  ---@param err string the error message
  function Future:error(err)
    if self.done then
      error('Tried to set an error on an already done future.')
    end

    self.results = pack(false, err)
    self.done = true
    for _, cb in ipairs(self.queue) do
      cb(unpack(self.results))
    end
    self.queue = {}
  end

  --- Waits for the future to be done.
  ---
  --- This function can be called in three different ways:
  ---
  --- - `await()`: This is a task function that yields until the future is done.
  --- - `await(cb)`: This calls the callback with the results of the task function when the future is done.
  function Future:await(cb)
    vim.validate('cb', cb, 'function', true)
    if cb and self.done then
      cb(unpack(self.results))
    elseif cb then
      table.insert(self.queue, cb)
    else
      local results = pack(self:pawait())
      if not results[1] then
        error(results[2], 0)
      end
      return unpack(results, 2, results.n)
    end
  end
  Future.__call = Future.await

  --- Waits for the future to be done.
  ---
  ---@param self Future the future
  ---@return boolean success whether the future was successful and the pawait was not cancelled.
  ---@return any ... the results of the task function or an error message.
  function Future:pawait()
    local running, yield_msg = true, ''
    if not self.done then
      local this = M.running()
      if this == nil then
        error('Future.pawait can only be used in a task.', 2)
      end

      table.insert(self.queue, function()
        if not running then
          -- This await was cancelled during yield. There’s nothing to resume.
          return
        end

        M.resume(this)
      end)

      running, yield_msg = M.pyield()
    end

    -- The await was cancelled.
    if not running then
      return false, yield_msg
    end

    return unpack(self.results)
  end

  --- Synchronously waits for the future to be done.
  ---
  --- This function uses busy waiting to wait for the future to be done.
  ---
  --- This function throws an error if the future ended with an error.
  ---
  ---@param timeout number The timeout in milliseconds.
  ---@param interval number The interval in milliseconds between checks.
  ---@return any ... The results of the coroutine function if done.
  function Future:wait(timeout, interval)

    -- can't wait in fast event, so just sleep. If we're in the main thread, this does nothing.
    M.sleep_until_nonfast()

    vim.wait(timeout, function()
      return self.done
    end, interval)

    if self.done then
      if self.results[1] then
        return unpack(self.results, 2, self.results.n)
      else
        error(self.results[2], 0)
      end
    else
      return
    end
  end

  function M.future()
    return setmetatable({ done = false, queue = {} }, Future)
  end

  local function wrap_future(fn)
    return function(self, ...)
      return fn(self.future, ...)
    end
  end

  ---@class Task
  --- A task is an extension of Lua coroutine.
  ---
  --- A task enhances Lua coroutine with:
  ---
  --- - A future that can be awaited.
  --- - The ability to capture errors.
  --- - The ability to cancel and handle cancellation.
  ---
  ---@field thread thread the coroutine thread
  ---@field future Future the future for the coroutine
  ---
  ---@field cancel fun(self: Task): boolean, ... cancels the task
  ---@field status fun(self: Task): string returns the task’s status
  ---@field resume fun(self: Task, ...): boolean, ... resumes the task
  ---@field is_done fun(self: Task): boolean returns true if the task is done
  ---
  ---@field cancelled boolean true if the user has requested cancellation
  ---
  ---@field await function awaits the task
  ---@field wait function waits the task
  ---@field pawait async fun(self: Task): boolean, ... awaits the task and returns errors
  local Task = {
    cancel = M.cancel,
    resume = M.resume,
    status = M.status,
    is_done = M.is_done,
    wait = wrap_future(Future.wait),
    await = wrap_future(Future.await),
    pawait = wrap_future(Future.pawait),
  }
  Task.__index = Task
  Task.__call = Task.await

  function Task:is_cancelled()
    return self.cancelled
  end

  function Task:unset_cancelled()
    self.cancelled = false
  end

  --- Creates a new task.
  ---
  ---@param tf function the task function
  ---@return Task
  function M.create(tf)
    local task = setmetatable({
      thread = coroutine.create(tf),
      future = M.future(),
      cancelled = false,
    }, Task)
    Tasks[task] = setmetatable({}, { __mode = 'k' })
    if running_task ~= nil then
      Tasks[running_task][task] = true
    end
    return task
  end
end

---@nodoc
---@class CbToTfOpts
---@field on_cancel? fun(args: table, ret: table) The function to call when the task is cancelled.
---                                    This can be used to stop allocation of resources.
---                                    This function receives packed tables with the original call’s arguments and
---                                    the immediately returned values (useful if the function returns a cancellable
---                                    handle).
---@field cleanup? fun(...) The function to call when the task has been cancelled but the callback gets called.
---                         This can be used to clean up allocated resources.
---                         This function receives the callback arguments.
---@field pos? number|'last' The position of the callback in the arguments. Defaults to 1.
---@field schedule? boolean Whether to schedule the callback. Defaults to false.

--- Converts a callback-based function to a task function.
---
--- If the callback is called asynchronously, then the task function yields exactly once and is resumed by whoever
--- calls the callback. If the callback is called synchronously, then the task function returns immediately.
---
--- This function is more involved than `cb_to_co` to handle the case of task cancellation. If the task is cancelled,
--- the user needs options to handle the eventual cleanup.
---
---@param f_cb function The function to convert. The callback needs to be its first argument.
---@param opts? CbToTfOpts The clean up options.
---@return async function tf A task function. Accepts the same arguments as f without the callback.
---                          Returns what f has passed to the callback.
function M.cb_to_tf(f_cb, opts)
  opts = opts or {}

  local function f_tf(...)
    local this = M.running()
    assert(this ~= nil, 'The result of cb_to_tf must be called within a task.')

    local f_status = 'running'
    local f_cb_ret = pack()
    -- f_cb needs to have the callback as its first argument, because varargs passing doesn’t work otherwise.
    local f_ret = pack(f_cb(function(...)
      if f_status == 'cancelled' then
        -- The task has been cancelled before this callback. Just run the cleanup function to cleanup
        -- any allocated resources.
        if opts.cleanup then
          opts.cleanup(...)
        end
        return
      end

      f_status = 'done'
      f_cb_ret = pack(...)
      if M.status(this) == 'suspended' then
        -- If we are suspended, then f_tf has yielded control after calling f_cb.
        -- Use the caller of this callback to resume computation until the next yield.
        M.resume(this)
      end
    end, ...))
    if f_status == 'running' then
      -- If we are here, then `f_cb` must not have called the callback yet, so it will do so asynchronously.
      -- Yield control and wait for the callback to resume it.
      local running, err_msg = M.pyield()
      if not running then
        f_status = 'cancelled'
        if opts.on_cancel then
          opts.on_cancel(pack(...), f_ret)
        end
        error(err_msg, 0)
      end
    end

    return unpack(f_cb_ret)
  end

  return f_tf
end

--- Transforms a coroutine function into a task function.
---
---@param f_co async function The coroutine function.
---@return async function tf The task function.
function M.co_to_tf(f_co)
  return function(...)
    local thread = coroutine.create(f_co)
    local results = pack(coroutine.resume(thread, ...))
    while true do
      if not results[1] then
        error(results[2], 0)
      end

      if coroutine.status(thread) == 'dead' then
        return unpack(results, 2, results.n)
      end

      -- The coroutine function has yielded, so we need to yield as well.
      results = pack(coroutine.resume(thread, M.yield(unpack(results, 2, results.n))))
    end
  end
end

return M
