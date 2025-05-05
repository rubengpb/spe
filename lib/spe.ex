defmodule Spe do
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
  def submit_job(pid, job), do: GenServer.call(pid, :submit_job)

  def init(opts) do
    num_workers = Keyword.get(opts, :num_workers, :unbounded)
    {:ok, %{num_workers: num_workers, current_workers: 0, pid_queued_jobs: [], jobs: %{}}}
  end

  def handle_call({:submit_job, %{"name" => name, "tasks" => tasks} = job}, _from, state) do
    job_id = 0
    {:reply, {:ok, job_id}, %{state | "jobs" => %{state["jobs"] | job_id => job}}}
  end

  def handle_call({:submit_job, _bad_job}, _from, state) do
    response = {:error, "Not name or tasks keys in the job"}
    {:reply, response, state}
  end
end
