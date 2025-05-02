defmodule Spe do
  @moduledoc """
  Documentation for `Spe`.
  """

  @doc """
  Hello world.

  ## Examples

      iex> Spe.hello()
      :world

  """
  def hello do
    :world
  end

  @spec star_link(list())

  def start_link(options) do
    server = spawn(fn -> spe_server(options, []) end)

    if is_pid(server) do
      {:ok, server}
    else
      {:error, "Something went wrong!"}
    end
  end

  @spec spe_server(list(), map())

  defp spe_server(options, current_jobs) do
    max_num_workers = Keyword.get(options, :num_workers)

    receive do
    end
  end

  def submit_job(job_description) do
    nombre = HashDict.get(job_description, :name)
    task_list = HashDict.get(job_description, :tasks)

    cond do
      !is_bitstring(nombre) -> {:error, "Nombre no es un string"}
      !is_list(task_list) -> {:error, "El conjunto de los trabajos no es una lista"}
    end
  end
end
