defmodule SPE do
  use GenServer
  @name __MODULE__

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: @name)
  def submit_job(job), do: GenServer.call(@name, {:submit_job, job})
  def start_job(job_id), do: GenServer.call(@name, {:start_job, job_id})

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
  def handle_call({:submit_job, %{"name" => name, "tasks" => tasks} = job}, _from, state) do
    cond do
      name == "" ->
        {:reply, {:error, "Empty name"}, state}

      tasks == [] ->
        {:reply, {:error, "Empty tasks"}, state}

      # Enum.any?(tasks, fn t -> "name" in Map.keys(t) end) ->
      #   {:reply, {:error, "Bad tasks"}, state}

      true ->
        job_id_int = state["next_id"]
        job_id = to_string(job_id_int)
        new_next_id = job_id_int + 1
        new_jobs = Map.put(state["jobs"], job_id, job)

        new_state = %{state | "next_id" => new_next_id, "jobs" => new_jobs}
        {:reply, {:ok, job_id}, new_state}
    end
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
        {:reply, {:ok, job_id}, state}
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

  def handle_info({:job_finished, job_id, result}, state) do
    IO.puts("##### STATE #######")
    IO.inspect(state)
    IO.puts("##### RESULT #######")
    IO.inspect(result)
    IO.puts("##### job_id #######")
    IO.inspect(job_id)
    # Phoenix.PubSub.broadcast(SPE.PubSub, job_id, {:succeeded, result})
    Phoenix.PubSub.broadcast(SPE.PubSub, job_id, {:succeeded, result})

    {:noreply, state}
  end
end
