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

    test "runs function with controller connection" do
      allow :kpro.connect_controller(any(), any()), return: {:ok, :connection}
      allow :kpro.close_connection(any()), return: :ok

      result =
        Util.with_connection([localhost: 9092], :controller, fn connection ->
          assert :connection == connection
          :return_value
        end)

      assert :return_value == result
      assert_called :kpro.connect_controller([{'localhost', 9092}], []), once()
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

  describe "chunk_by_byte_size" do
    test "will create chunks that are less then suppliec chunk_byte_size" do
      chunks =
        ?a..?z
        |> Enum.map(&List.to_string([&1]))
        |> Elsa.Util.chunk_by_byte_size(10 * 10)

      assert length(chunks) == 3
      assert Enum.at(chunks, 0) == ?a..?i |> Enum.map(&List.to_string([&1]))
      assert Enum.at(chunks, 1) == ?j..?r |> Enum.map(&List.to_string([&1]))
      assert Enum.at(chunks, 2) == ?s..?z |> Enum.map(&List.to_string([&1]))
    end

    test "will create chunks of for {key, value} pairs" do
      chunks =
        ?a..?z
        |> Enum.map(&to_tuple/1)
        |> Elsa.Util.chunk_by_byte_size(20 + 10 * 10)

      assert length(chunks) == 3
      assert Enum.at(chunks, 0) == ?a..?i |> Enum.map(&to_tuple/1)
      assert Enum.at(chunks, 1) == ?j..?r |> Enum.map(&to_tuple/1)
      assert Enum.at(chunks, 2) == ?s..?z |> Enum.map(&to_tuple/1)
    end
  end

  defp to_tuple(char) do
    string = List.to_string([char])
    {string, string}
  end
end
