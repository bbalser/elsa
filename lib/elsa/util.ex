defmodule Elsa.Util do
  @moduledoc false

  @default_max_chunk_size 900_000

  def with_connection(endpoints, type \\ :any, fun) when is_function(fun) do
    endpoints
    |> reformat_endpoints()
    |> connect(type)
    |> do_with_connection(fun)
  end

  def reformat_endpoints(endpoints) do
    Enum.map(endpoints, fn {key, value} -> {to_charlist(key), value} end)
  end

  def get_api_version(connection, api) do
    {:ok, api_versions} = :kpro.get_api_versions(connection)
    {_, version} = Map.get(api_versions, api)
    version
  end

  def start_client(endpoints, name) do
    endpoints
    |> reformat_endpoints()
    |> :brod.start_link_client(name)
    |> case do
      {:ok, client_pid} ->
        {:ok, client_pid}

      {:error, {:already_started, client_pid}} ->
        {:ok, client_pid}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def chunk_by_byte_size(collection, chunk_byte_size \\ @default_max_chunk_size, function \\ &get_byte_size/1) do
    collection
    |> Enum.chunk_while({0, []}, &chunk(&1, &2, chunk_byte_size, function), &after_chunk/1)
  end

  defp connect(endpoints, :controller), do: :kpro.connect_controller(endpoints, [])
  defp connect(endpoints, _type), do: :kpro.connect_any(endpoints, [])

  defp do_with_connection({:ok, connection}, fun) do
    fun.(connection)
  after
    :kpro.close_connection(connection)
  end

  defp do_with_connection({:error, reason}, _fun) do
    raise Elsa.ConnectError, message: format_reason(reason)
  end

  defp format_reason(reason) do
    cond do
      is_binary(reason) -> reason
      Exception.exception?(reason) -> Exception.format(:error, reason)
      true -> inspect(reason)
    end
  end

  defp chunk(item, {current_size, current_batch}, chunk_byte_size, function) do
    item_size = function.(item)
    new_total = current_size + item_size

    case new_total < chunk_byte_size do
      true -> add_item_to_batch(new_total, item, current_batch)
      false -> finish_batch(item_size, item, current_batch)
    end
  end

  defp add_item_to_batch(total, item, batch) do
    {:cont, {total, [item | batch]}}
  end

  defp finish_batch(total, item, batch) do
    {:cont, Enum.reverse(batch), {total, [item]}}
  end

  defp after_chunk({_size, []}) do
    {:cont, {0, []}}
  end

  defp after_chunk({_size, current_batch}) do
    finish_batch(0, nil, current_batch)
  end

  defp get_byte_size({one, two}) do
    byte_size(one) + byte_size(two)
  end

  defp get_byte_size(item) do
    byte_size(item)
  end
end
