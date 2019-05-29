defmodule Elsa.UtilTest do
  use ExUnit.Case
  use Placebo
  import Checkov

  alias Elsa.Util

  describe "with_connection/2" do
    test "runs function with connection" do
      allow :kpro.connect_any(any(), any()), return: {:ok, :connection}
      allow :kpro.close_connection(any()), return: :ok

      result =
        Util.with_connection([localhost: 9092], fn connection ->
          assert :connection == connection
          :return_value
        end)

      assert :return_value == result
      assert_called :kpro.connect_any([{'localhost', 9092}], []), once()
      assert_called :kpro.close_connection(:connection), once()
    end

    test "calls close_connection when fun raises an error" do
      allow :kpro.connect_any(any(), any()), return: {:ok, :connection}
      allow :kpro.close_connection(any()), return: :ok

      try do
        Util.with_connection([localhost: 9092], fn _connection ->
          raise "some error"
        end)

        flunk("Should have raised error")
      rescue
        e in RuntimeError -> assert Exception.message(e) == "some error"
      end

      assert_called :kpro.close_connection(:connection), once()
    end

    data_test "raises exception when unable to create connection" do
      allow :kpro.connect_any(any(), any()), return: {:error, reason}

      assert_raise(Elsa.ConnectError, message, fn ->
        Util.with_connection([{'localhost', 9092}], fn _connection -> nil end)
      end)

      refute_called :kpro.close_connection(any())

      where([
        [:reason, :message],
        ["unable to connect", "unable to connect"],
        [{:econnrefused, [1, 2]}, inspect({:econnrefused, [1, 2]})],
        [RuntimeError.exception("jerks"), Exception.format(:error, RuntimeError.exception("jerks"))]
      ])
    end
  end
end
