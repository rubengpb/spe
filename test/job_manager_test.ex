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
  end
end
