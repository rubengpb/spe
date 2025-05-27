defmodule JobManagetTest do
  use ExUnit.Case, async: false

  test "submit_bad_jobs" do
    task1 = %{
      "name" => "t1",
      "exec" => fn _ -> 1 + 2 end,
      "timeout" => :infinity,
      "enables" => ["t2"]
    }

    task2 = %{
      "name" => "t2",
      "exec" => fn %{"t1" => val1} -> val1 + 1 end,
      "timeout" => :infinity,
      "enables" => []
    }

    job1 = %{"name" => "job1", "tasks" => [task1, task2]}
    {:ok, _pid} = SPE.start_link(num_workers: 4)
    {:ok, pid_submitted} = SPE.submit_job(job1)
    SPE.start_job(pid_submitted)
  end

  test "case of enun" do
    task1 = %{
      "name" => "task1",
      "exec" => fn _ -> 1 + 2 end,
      "timeout" => :infinity,
      "enables" => ["task3"]
    }

    task2 = %{
      "name" => "task2",
      "exec" => fn _ -> 3 + 4 end,
      "timeout" => :infinity,
      "enables" => ["task4"]
    }

    task3 = %{
      "name" => "task3",
      "exec" => fn %{"task1" => val1} -> val1 + 2 end,
      "timeout" => :infinity,
      "enables" => ["task5"]
    }

    task4 = %{
      "name" => "task4",
      "exec" => fn %{"task2" => val2} -> val2 * 3 end,
      "timeout" => :infinity,
      "enables" => ["task5"]
    }

    task5 = %{
      "name" => "task5",
      "exec" => fn %{"task2" => val2, "task3" => val3, "task4" => val4} ->
        IO.puts("value: #{inspect(val2 + val3 + val4)}")
      end,
      "timeout" => :infinity,
      "enables" => []
    }

    task6 = %{
      "name" => "task6",
      "exec" => fn _ -> IO.puts("hello") end,
      "timeout" => :infinity,
      "enables" => []
    }

    job1 = %{"name" => "job1", "tasks" => [task1, task2, task3, task4, task5, task6]}
    job2 = %{"name" => "job2", "tasks" => [task6]}
    {:ok, _pid} = SPE.start_link(num_workers: 4)
    {:ok, pid_submitted_1} = SPE.submit_job(job1)
    {:ok, pid_submitted_2} = SPE.submit_job(job2)
    SPE.start_job(pid_submitted_1)
    SPE.start_job(pid_submitted_2)
  end
end
