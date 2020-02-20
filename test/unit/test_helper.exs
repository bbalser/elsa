ExUnit.start(exclude: [:skip])

defmodule AsyncAssertion do
  require Logger
  import ExUnit.Assertions

  def assert_async(retries \\ 10, delay \\ 200, fun)

  def assert_async(1, _delay, fun) when is_function(fun, 0) do
    assert fun.()
  end

  def assert_async(retries, delay, fun) when is_function(fun, 0) do
    case fun.() do
      false ->
        Process.sleep(delay)
        assert_async(retries - 1, delay, fun)

      true ->
        :ok
    end
  rescue
    e in ExUnit.AssertionError ->
      Logger.warn("Retries Remaining #{retries}\n" <> ExUnit.AssertionError.message(e))
      Process.sleep(delay)
      assert_async(retries - 1, delay, fun)
  end
end

defmodule TestHelper do
  import ExUnit.Assertions

  def assert_down(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :shutdown)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
