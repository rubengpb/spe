defmodule SPE.JobManager do
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def get_results(pid), do: GenServer.call(pid, :results)
  def get_tasks(pid), do: GenServer.call(pid, :tasks)

  #def start_one_job(pid), do: send(pid, :start_one_job)

  @impl true
  def init(opts) do
    tasks = normalize_tasks(opts["tasks"])
    server_pid = opts["server_pid"]

    {ready, blocked} = split_ready(tasks, %{})
    Enum.each(ready, fn _ -> send(server_pid, {:existing_worker, self()}) end)

    state = %{
      "job_id" => opts["job_id"],
      "name" => opts["name"],
      "server_pid" => server_pid,
      "queued" => ready,
      "blocked" => blocked,
      "running" => MapSet.new(),
      "status" => :succeeded,
      "results" => %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_info(:start_one_job, state) do
    [task | rest] = state["queued"]

    input =
      Enum.reduce(state["results"], %{}, fn {k, value}, acc ->
        case value do
          {:result, new_value} -> Map.put(acc, k, new_value)
          _ -> acc
        end
      end)

    job_id = state["job_id"]

    Phoenix.PubSub.local_broadcast(
      SPE.PubSub,
      job_id,
      {:spe, :erlang.monotonic_time(:millisecond), {job_id, :task_started, task["name"]}}
    )

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
      |> Map.put("queued", rest)
      |> Map.update!("running", &MapSet.put(&1, task["name"]))

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:task_finished, name, {:result, value}}, state) do
    send(state["server_pid"], :one_job_finished)
    job_id = state["job_id"]

    Phoenix.PubSub.local_broadcast(
      SPE.PubSub,
      job_id,
      {:spe, :erlang.monotonic_time(:millisecond), {job_id, :task_terminated, name}}
    )

    state
    |> add_result(name, {:result, value})
    |> schedule_new_ready()
    |> maybe_done()
  end

  def handle_info({:task_finished, name, {:failed, reason}}, state) do
    send(state["server_pid"], :one_job_finished)
    job_id = state["job_id"]

    Phoenix.PubSub.local_broadcast(
      SPE.PubSub,
      job_id,
      {:spe, :erlang.monotonic_time(:millisecond), {job_id, :task_terminated, name}}
    )

    state
    |> add_result(name, {:failed, reason})
    |> discard_dependents_failures()
    |> schedule_new_ready()
    |> Map.put("status", :failed)
    |> maybe_done()
  end

  @impl true
  def handle_call(:results, _from, state) do
    {:reply, state["results"], state}
  end

  def handle_call(:tasks, _from, state) do
    {:reply, state["tasks"], state}
  end

  defp normalize_tasks(tasks) do
    Enum.map(tasks, fn task ->
      deps =
        tasks
        |> Enum.filter(fn x -> task["name"] in x["enables"] end)
        |> Enum.map(& &1["name"])

      Map.put(task, "deps", deps)
    end)
  end

  defp add_result(state, name, result) do
    %{state
      | "running" => MapSet.delete(state["running"], name),
        "results" => Map.put(state["results"], name, result)
    }
  end

  defp split_ready(tasks, results) do
    split_with(tasks, &ready?/2, results)
  end

  defp ready?(task, results) do
    Enum.all?(task["deps"], fn dep ->
      case Map.fetch(results, dep) do
        {:ok, :failed} -> false
        {:ok, :not_run} -> false
        {:ok, _value} -> true
        :error -> false
      end
    end)
  end

  defp schedule_new_ready(state) do
    {new_ready, still_blocked} = split_ready(state["blocked"], state["results"])

    Enum.each(new_ready, fn _ -> send(state["server_pid"], {:existing_worker, self()}) end)

    %{state | "queued" => state["queued"] ++ new_ready, "blocked" => still_blocked}
  end

  defp discard_dependents_failures(state) do
    {blocked_kept, _failed_now, results} =
      Enum.reduce(state["blocked"], {[], [], state["results"]}, fn task, {keep, fail, res} ->
        if Enum.any?(task["deps"], &(res[&1] == :not_run or match?({:failed, _}, res[&1]))) do
          {
            keep,
            [task["name"] | fail],
            Map.put(res, task["name"], :not_run)
          }
        else
          {[task | keep], fail, res}
        end
      end)

    %{state | "blocked" => blocked_kept, "results" => results}
  end

  defp maybe_done(%{"blocked" => [], "queued" => [], "running" => running} = state) do
    if MapSet.size(running) == 0 do
      send(state["server_pid"], {:all_jobs_finished, state["job_id"], state["results"]})
      job_id = state["job_id"]

      Phoenix.PubSub.local_broadcast(
        SPE.PubSub,
        job_id,
        {:spe, :erlang.monotonic_time(:millisecond),
         {job_id, :result, {state["status"], state["results"]}}}
      )
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
