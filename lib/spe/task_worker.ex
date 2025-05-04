defmodule SPE.TaskWorker do
  def start_link(name, fun, input, timeout, caller) do
    Task.start_link(fn -> run(name, fun, input, timeout, caller) end)
  end

  defp run(name, fun, input, timeout, caller) do
    result =
      case timeout do
        :infinity ->
          run_safe(fun, input)

        ms ->
          task = Task.async(fn -> run_safe(fun, input) end)
          Task.yield(task, ms) || Task.shutdown(task, :brutal_kill) || {:failed, :timeout}
      end

    send(caller, {:task_finished, name, result})
  end

  defp run_safe(fun, input) do
    try do
      {:result, fun.(input)}
    rescue
      e -> {:failed, {:crashed, Exception.message(e)}}
    catch
      :exit, r -> {:failed, {:crashed, r}}
      e -> {:failed, {:crashed, e}}
    end
  end
end
