defmodule AssertAsync do
  defmodule Impl do
    require Logger

    @defaults %{
      sleep: 200,
      max_tries: 10,
      debug: false
    }

    def assert(function, opts) do
      state = Map.merge(@defaults, Map.new(opts))
      do_assert(function, state)
    end

    defp do_assert(function, %{max_tries: 1}) do
      function.()
    end

    defp do_assert(function, %{max_tries: max_tries} = opts) do
      try do
        function.()
      rescue
        e in ExUnit.AssertionError ->
          if opts.debug do
            Logger.debug(fn ->
              "AssertAsync(remaining #{max_tries - 1}): #{ExUnit.AssertionError.message(e)}"
            end)
          end

          Process.sleep(opts.sleep)
          do_assert(function, %{opts | max_tries: max_tries - 1})
      end
    end
  end

  defmacro assert_async(opts \\ [], do: do_block) do
    quote do
      AssertAsync.Impl.assert(fn -> unquote(do_block) end, unquote(opts))
    end
  end
end
