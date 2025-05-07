defmodule SPE.TaskWorker do
  def start_link(name, fun, input, timeout, caller) do
    Task.start(fn -> run(name, fun, input, timeout, caller) end)
  end

  defp run(name, fun, input, timeout, caller) do
    result =
      case timeout do
        :infinity ->
          execute_without_timeout(fun, input)

        ms when is_integer(ms) ->
          execute_with_timeout(fun, input, ms)
      end

    send(caller, {:task_finished, name, result})
  end

  defp execute_without_timeout(fun, input) do
    id = make_ref()
    parent = self()

    {pid, ref} =
      spawn_monitor(fn ->
        result = run_safe(fun, input)
        send(parent, {id, result})
      end)

    receive do
      {^id, result} ->
        result

      {:DOWN, ^ref, :process, ^pid, reason} ->
        {:failed, {:crashed, reason}}
    end
  end

  defp execute_with_timeout(fun, input, ms) do
    task = Task.async(fn -> execute_without_timeout(fun, input) end)

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

  defp run_safe(fun, input) do
    try do
      {:result, fun.(input)}
    rescue
      e ->
        {:failed, {:crashed, Exception.message(e)}}
    catch
      _kind, error ->
        {:failed, {:crashed, error}}
    end
  end
end
