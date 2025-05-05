defmodule SPE do
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
  def submit_job(pid, job), do: GenServer.call(pid, {:submit_job, job})
  def start_job(pid, job_id), do: GenServer.call(pid, {:start_job, job_id})

  @impl true
  def init(opts) do
    num_workers = Keyword.get(opts, :num_workers, :unbounded)

    {:ok,
     %{
       "num_workers" => num_workers,
       "current_workers" => 0,
       "pid_queued_jobs" => [],
       "jobs" => %{},
       "next_id" => 0
     }}
  end

  @impl true
  def handle_call({:submit_job, %{"name" => _name, "tasks" => _tasks} = job}, _from, state) do
    job_id = state["next_id"]
    new_next_id = job_id + 1
    new_jobs = Map.put(state["jobs"], job_id, job)

    new_state = %{state | "next_id" => new_next_id, "jobs" => new_jobs}
    {:reply, {:ok, job_id}, new_state}
  end

  @impl true
  def handle_call({:submit_job, _bad_job}, _from, state) do
    response = {:error, "Not name or tasks keys in the job"}
    {:reply, response, state}
  end

  @impl true
  def handle_call({:start_job, job_id}, _from, state) do
    IO.inspect(state)

    case Map.get(state["jobs"], job_id) do
      nil ->
        response = {:error, "Not exist job with this id: #{job_id}"}
        {:reply, response, state}

      job ->
        normalize_job = Map.put(job, "job_id", job_id)
        normalize_job = Map.put(normalize_job, "server_pid", self())
        SPE.JobManager.start_link(normalize_job)
        Map.delete(state["jobs"], job_id)
        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_info({:there_is_worker, job_pid}, state) do
    new_state =
      case state["current_workers"] < state["num_workers"] do
        true ->
          send(job_pid, :throw_one)
          %{state | "current_workers" => state["current_workers"] + 1}

        _ ->
          %{state | "pid_queued_jobs" => state["pid_queued_jobs"] ++ [job_pid]}
      end

    {:noreply, new_state}
  end

  @impl true
  def handle_info(:finished_one, state) do
    new_state =
      case state["pid_queued_jobs"] do
        [] ->
          %{state | "current_workers" => state["current_workers"] - 1}

        [pid | rest] ->
          send(pid, :throw_one)
          %{state | "pid_queued_jobs" => rest, "current_workers" => state["current_workers"] - 1}
      end

    {:noreply, new_state}
  end

  def handle_info({:job_finished, _pid, result}, state) do
    IO.puts("##### STATE #######")
    IO.inspect(state)
    IO.puts("##### RESULT #######")
    IO.inspect(result)

    {:noreply, state}
  end
end
