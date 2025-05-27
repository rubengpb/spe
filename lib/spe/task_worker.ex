defmodule SPE.TaskWorker do
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
      {:ok, result} -> result
      {:exit, reason} -> {:failed, {:crashed, reason}}
      nil ->
        Task.shutdown(task, :brutal_kill)
        {:failed, :timeout}
    end
  end

  defp execute_without_timeout(function, input) do
    id = make_ref()
    parent = self()

    {pid, ref} =
      spawn_monitor(fn ->
        result = safe_execution(function, input)
        send(parent, {id, result})
      end)

    receive do
      {^id, result} -> result
      {:DOWN, ^ref, :process, ^pid, reason} -> {:failed, {:crashed, reason}}
    end
  end

  defp safe_execution(function, input) do
    try do
      {:result, function.(input)}
    rescue
      e -> {:failed, {:crashed, Exception.message(e)}}
    catch
      _kind, error -> {:failed, {:crashed, error}}
    end
  end
end
