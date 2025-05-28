defmodule SPE do
  @moduledoc """
  Main server responsible for job submission, validation, and dispatching.
  """

  use GenServer
  @name __MODULE__

  ## Public API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: @name)

  @spec submit_job(map()) :: {:ok, String.t()} | {:error, String.t()}
  def submit_job(job), do: GenServer.call(@name, {:submit_job, job})

  @spec start_job(String.t()) :: {:ok, String.t()} | {:error, String.t()}
  def start_job(job_id), do: GenServer.call(@name, {:start_job, job_id})

  ## GenServer Callbacks

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
      Enum.map(tasks, fn task ->
        task
        |> Map.put_new("timeout", :infinity)
        |> Map.put_new("enables", [])
      end)

    cond do
      not is_binary(name) ->
        {:reply, {:error, "Name must be a string"}, state}

      name == "" ->
        {:reply, {:error, "Empty name"}, state}

      new_tasks == [] ->
        {:reply, {:error, "Empty tasks"}, state}

      Enum.any?(new_tasks, &invalid_task?/1) ->
        {:reply, {:error, "Invalid task structure"}, state}

      not valid_task_names?(new_tasks) ->
        {:reply, {:error, "Invalid task names or dependencies"}, state}

      true ->
        job_id = Integer.to_string(state["next_unique_id"])
        updated_jobs = Map.put(state["jobs"], job_id, %{job | "tasks" => new_tasks})
        new_state =
          state
          |> Map.put("jobs", updated_jobs)
          |> Map.update!("next_unique_id", &(&1 + 1))

        {:reply, {:ok, job_id}, new_state}
    end
  end

  @impl true
  def handle_call({:submit_job, _invalid_job}, _from, state) do
    {:reply, {:error, "Missing or invalid name/tasks in submit_job"}, state}
  end

  @impl true
  def handle_call({:start_job, job_id}, _from, state) do
    case Map.get(state["jobs"], job_id) do
      nil ->
        {:reply, {:error, "Job with ID #{job_id} does not exist"}, state}

      job ->
        normalized_job =
          job
          |> Map.put("job_id", job_id)
          |> Map.put("server_pid", self())
          |> Map.put_new("timeout", :infinity)
          |> Map.put_new("enables", [])

        SPE.JobManager.start_link(normalized_job)
        {:reply, {:ok, job_id}, state}
    end
  end

  @impl true
  def handle_info({:existing_worker, job_pid}, state) do
    cond do
      state["current_workers"] < state["max_workers"] ->
        send(job_pid, :start_one_job)
        {:noreply, Map.update!(state, "current_workers", &(&1 + 1))}

      true ->
        new_queue = state["pid_queued_jobs"] ++ [job_pid]
        {:noreply, Map.put(state, "pid_queued_jobs", new_queue)}
    end
  end

  @impl true
  def handle_info(:one_job_finished, state) do
    case state["pid_queued_jobs"] do
      [] ->
        {:noreply, Map.update!(state, "current_workers", &(&1 - 1))}

      [next_pid | rest] ->
        send(next_pid, :start_one_job)

        new_state =
          state
          |> Map.put("pid_queued_jobs", rest)

        {:noreply, new_state}
    end
  end

  @impl true
  def handle_info({:all_jobs_finished, _job_id, _result}, state), do: {:noreply, state}

  ## Helpers

  defp invalid_task?(%{
         "name" => name,
         "exec" => exec,
         "timeout" => timeout,
         "enables" => enables
       }) do
    not (is_binary(name) and name != "" and is_function(exec) and
           (timeout == :infinity or is_integer(timeout)) and is_list(enables))
  end

  defp invalid_task?(_), do: true

  defp valid_task_names?(tasks) do
    names = Enum.map(tasks, & &1["name"])
    enables = Enum.flat_map(tasks, & &1["enables"]) |> Enum.uniq()

    Enum.uniq(names) == names and Enum.all?(enables, &(&1 in names))
  end
end
