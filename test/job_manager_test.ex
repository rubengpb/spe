defmodule JobManagetTest do
  use ExUnit.Case, async: false

  test "submit_bad_jobs" do
    task1 = %{
      "name" => "t1",
      "exec" => fn _ -> 1 + 2 end,
      "timeout" => :infinity,
      "enable" => ["t2"]
    }

    task2 = %{
      "name" => "t2",
      "exec" => fn %{"t1" => val1} -> val1 + 1 end,
      "timeout" => :infinity,
      "enable" => []
    }

    job1 = %{"name" => "job1", "tasks" => [task1, task2]}
    {:ok, pid} = SPE.start_link(num_workers: 4)
    SPE.submit_job(pid, job1)
    SPE.start_job(pid, 0)
  end

  test "case of enun" do
    task1 = %{
      "name" => "task1",
      "exec" => fn _ -> 1 + 2 end,
      "timeout" => :infinity,
      "enable" => ["task3"]
    }

    task2 = %{
      "name" => "task2",
      "exec" => fn _ -> 3 + 4 end,
      "timeout" => :infinity,
      "enable" => ["task4"]
    }

    task3 = %{
      "name" => "task3",
      "exec" => fn %{"task1" => val1} -> val1 + 2 end,
      "timeout" => :infinity,
      "enable" => ["task5"]
    }

    task4 = %{
      "name" => "task4",
      "exec" => fn %{"task2" => val2} -> val2 * 3 end,
      "timeout" => :infinity,
      "enable" => ["task5"]
    }

    task5 = %{
      "name" => "task5",
      "exec" => fn %{"task2" => val2, "task3" => val3, "task4" => val4} ->
        IO.puts("value: #{inspect(val2 + val3 + val4)}")
      end,
      "timeout" => :infinity,
      "enable" => []
    }

    task6 = %{
      "name" => "task6",
      "exec" => fn _ -> IO.puts("hello") end,
      "timeout" => :infinity,
      "enable" => []
    }

    job1 = %{"name" => "job1", "tasks" => [task1, task2, task3, task4, task5, task6]}
    job2 = %{"name" => "job2", "tasks" => [task6]}
    {:ok, pid} = SPE.start_link(num_workers: 4)
    SPE.submit_job(pid, job1)
    SPE.submit_job(pid, job2)
    SPE.start_job(pid, 0)
    SPE.start_job(pid, 1)
  end
end
