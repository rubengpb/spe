defmodule SPE.TaskWorker do
  @moduledoc """
  Executes a function with optional timeout and reports the result to a caller.
  """

  @type result ::
          {:result, any()}
          | {:failed, :timeout}
          | {:failed, {:crashed, any()}}

  @spec start_link(any(), (any() -> any()), any(), timeout(), pid()) :: {:ok, pid()}
  def start_link(name, function, input, timeout, caller) do
    Task.start(fn -> run(name, function, input, timeout, caller) end)
  end

  defp run(name, function, input, timeout, caller) do
    result =
      case timeout do
        :infinity -> execute_without_timeout(function, input)
        ms when is_integer(ms) -> execute_with_timeout(function, input, ms)
      end

    send(caller, {:task_finished, name, result})
  end

  defp execute_with_timeout(function, input, ms) do
    task = Task.async(fn -> execute_without_timeout(function, input) end)

    case Task.yield(task, ms) do
      {:ok, result} ->
        result

      {:exit, reason} ->
        {:failed, {:crashed, reason}}

      nil ->
        Task.shutdown(task, :brutal_kill)
        {:failed, :timeout}
    end
  end

  defp execute_without_timeout(function, input) do
    ref_id = make_ref()
    parent = self()

    {pid, monitor_ref} =
      spawn_monitor(fn ->
        result = safe_execution(function, input)
        send(parent, {ref_id, result})
      end)

    receive do
      {^ref_id, result} ->
        result

      {:DOWN, ^monitor_ref, :process, ^pid, reason} ->
        {:failed, {:crashed, reason}}
    end
  end

  defp safe_execution(function, input) do
    try do
      {:result, function.(input)}
    rescue
      exception -> {:failed, {:crashed, Exception.message(exception)}}
    catch
      kind, reason -> {:failed, {:crashed, {kind, reason}}}
    end
  end
end
