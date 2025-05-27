defmodule SPE do
  use GenServer
  @name __MODULE__

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: @name)
  def submit_job(job), do: GenServer.call(@name, {:submit_job, job})
  def start_job(job_id), do: GenServer.call(@name, {:start_job, job_id})

  @impl true
  def init(opts) do
    max_workers = Keyword.get(opts, :num_workers, :unbounded)

    {:ok,
     %{
       "max_workers" => max_workers,
       "current_workers" => 0,
       "pid_queued_jobs" => [],
       "jobs" => %{},
       "next_unique_id" => 0
     }}
  end

  @impl true
  def handle_call({:submit_job, %{"name" => name, "tasks" => tasks} = job}, _from, state) do
    new_tasks =
      Enum.map(tasks, fn t ->
        t |> Map.put_new("timeout", :infinity) |> Map.put_new("enables", [])
      end)

    cond do
      not is_binary(name) ->
        {:reply, {:error, "Name must be an string"}, state}

      name == "" ->
        {:reply, {:error, "Empty name"}, state}

      new_tasks == [] ->
        {:reply, {:error, "Empty tasks"}, state}

      Enum.any?(new_tasks, fn t ->
        not good_task(t)
      end) ->
        {:reply, {:error, "Bad tasks"}, state}

      not correct_names(new_tasks) ->
        {:reply, {:error, "Bad names of tasks"}, state}

      true ->
        job_id_int = state["next_unique_id"]
        job_id_str = to_string(job_id_int)
        new_next_unique_id = job_id_int + 1
        new_jobs = Map.put(state["jobs"], job_id_str, %{job | "tasks" => new_tasks})

        new_state = %{state | "next_unique_id" => new_next_unique_id, "jobs" => new_jobs}
        {:reply, {:ok, job_id_str}, new_state}
    end
  end

  @impl true
  def handle_call({:submit_job, _bad_job}, _from, state) do
    response = {:error, "Empty name or tasks keys in sumbit_job"}
    {:reply, response, state}
  end

  @impl true
  def handle_call({:start_job, job_id}, _from, state) do
    case Map.get(state["jobs"], job_id) do
      nil ->
        response = {:error, "Job with id: #{job_id} does not exist"}
        {:reply, response, state}

      job ->
        normalize_job =
          Map.put(job, "job_id", job_id)
          |> Map.put("server_pid", self())
          |> Map.put_new("timeout", :infinity)
          |> Map.put_new("enables", [])

        SPE.JobManager.start_link(normalize_job)
        {:reply, {:ok, job_id}, state}
    end
  end

  @impl true
  def handle_info({:existing_worker, job_pid}, state) do
    new_state =
      case state["current_workers"] < state["max_workers"] do
        true ->
          send(job_pid, :start_one_job)
          %{state | "current_workers" => state["current_workers"] + 1}

        _ ->
          %{state | "pid_queued_jobs" => state["pid_queued_jobs"] ++ [job_pid]}
      end

    {:noreply, new_state}
  end

  @impl true
  def handle_info(:finished_one_job, state) do
    new_state =
      case state["pid_queued_jobs"] do
        [] ->
          %{state | "current_workers" => state["current_workers"] - 1}

        [pid | rest] ->
          send(pid, :start_one_job)
          %{state | "pid_queued_jobs" => rest}
      end
    {:noreply, new_state}
  end

  def handle_info({:job_finished, _job_id, _result}, state) do
    {:noreply, state}
  end

  defp good_task(
         %{"name" => name, "exec" => exec, "timeout" => timeout, "enables" => enables} = _task
       ) do
    is_binary(name) && name != "" && is_function(exec) &&
      (timeout == :infinity or is_integer(timeout)) &&
      is_list(enables)
  end

  defp good_task(_task), do: false

  defp correct_names(tasks) do
    enables =
      Enum.reduce(tasks, [], fn %{"enables" => enables}, acc -> enables ++ acc end)
      |> Enum.uniq()

    names =
      Enum.map(tasks, fn t -> t["name"] end)

    exit_enables =
      Enum.all?(enables, fn e -> e in names end)

    different_names =
      Enum.uniq(names) == names

    exit_enables && different_names
  end
end
