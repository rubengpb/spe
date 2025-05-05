defmodule SPE.JobManager do
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: opts[:name])

  def get_results(pid), do: GenServer.call(pid, :results)

  def throw_one(pid), do: send(pid, :throw_one)

  @impl true
  def init(opts) do
    tasks = Enum.map(opts["tasks"], &normalize_task/1)
    server_pid = opts["server_pid"]

    {ready, blocked} = split_ready(tasks, %{})

    Enum.each(ready, fn _ -> send(server_pid, {:there_is_worker, self()}) end)

    state = %{
      "job_id" => opts["job_id"],
      "name" => opts["name"],
      "server_pid" => server_pid,
      "queued" => ready,
      "blocked" => blocked,
      "running" => MapSet.new(),
      "results" => %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_info(:throw_one, state) do
    [task | rest] = state["queued"]

    input = build_input(task, state["results"])

    {:ok, _pid} =
      SPE.TaskWorker.start_link(
        task["name"],
        task["exec"],
        input,
        task["timeout"],
        self()
      )

    new_state =
      state
      |> Map.put(:queued, rest)
      |> Map.update!("running", &MapSet.put(&1, task["name"]))

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:task_finished, name, {:result, value}}, state) do
    send(state["server_pid"], :finished_one)

    state
    |> add_result(name, value)
    |> schedule_new_ready()
    |> maybe_done()
  end

  def handle_info({:task_finished, name, {:failed, _reason}}, state) do
    send(state["server_pid"], :finished_one)

    state
    |> add_result(name, :failed)
    |> discard_dependents_failures()
    |> schedule_new_ready()
    |> maybe_done()
  end

  @impl true
  def handle_call(:results, _from, state) do
    {:reply, state["results"], state}
  end

  defp normalize_task(task),
    do: Map.put_new(task, "deps", [])

  defp add_result(state, name, result) do
    %{
      state
      | running: MapSet.delete(state["running"], name),
        results: Map.put(state["results"], name, result)
    }
  end

  defp build_input(task, results) do
    Enum.into(task["deps"], %{}, fn dep -> {dep, results[dep]} end)
  end

  defp split_ready(tasks, results) do
    split_with(tasks, &ready?/2, results)
  end

  defp ready?(task, results) do
    Enum.all?(task["deps"], fn dep ->
      case Map.fetch(results, dep) do
        {:ok, :failed} -> false
        {:ok, _value} -> true
        :error -> false
      end
    end)
  end

  defp schedule_new_ready(state) do
    {new_ready, still_blocked} = split_ready(state["blocked"], state["results"])

    Enum.each(new_ready, fn _ -> send(state["server_pid"], {:there_is_worker, self()}) end)

    %{state | queued: state["queued"] ++ new_ready, blocked: still_blocked}
  end

  defp discard_dependents_failures(state) do
    {blocked_kept, _failed_now, results} =
      Enum.reduce(state["blocked"], {[], [], state["results"]}, fn task, {keep, fail, res} ->
        if Enum.any?(task["deps"], &(res[&1] == :failed)) do
          {
            keep,
            [task["name"] | fail],
            Map.put(res, task["name"], :failed)
          }
        else
          {[task | keep], fail, res}
        end
      end)

    %{state | blocked: blocked_kept, results: results}
  end

  defp maybe_done(%{"blocked" => [], "queued" => [], "running" => running} = state) do
    if MapSet.size(running) == 0 do
      send(state["server_pid"], {:job_finished, self(), state["results"]})
    end

    {:noreply, state}
  end

  defp maybe_done(state), do: {:noreply, state}

  defp split_with(list, fun, results) do
    Enum.reduce(list, {[], []}, fn x, {true_acc, false_acc} ->
      if fun.(x, results), do: {[x | true_acc], false_acc}, else: {true_acc, [x | false_acc]}
    end)
    |> then(fn {t, f} -> {Enum.reverse(t), Enum.reverse(f)} end)
  end
end
