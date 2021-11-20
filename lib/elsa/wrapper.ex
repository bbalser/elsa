defmodule Elsa.Wrapper do
  @moduledoc """
  Provides a supervisable wrapper for the Elsa supervision
  tree to manage brod producers and consumers. Provides
  convenience functions for starting producer and consumer
  processes directly without the default supervisors brod
  interposes between them and the application.

  By taking over supervision of the processes directly interacting
  with Kafka by way of a brod client, these processes can be
  registered independently from the client process so in the event
  of the client dropping its connection, the record of what
  processes should be producing or consuming from what topics and
  partitions can be immediately reconstructed instead of being dropped.
  """
  use GenServer
  require Logger

  @default_delay 5_000
  @default_kill_timeout 5_000

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  def init(args) do
    Process.flag(:trap_exit, true)

    state = %{
      mfa: Keyword.fetch!(args, :mfa),
      started: :erlang.system_time(:milli_seconds),
      delay: Keyword.get(args, :delay, @default_delay),
      kill_timeout: Keyword.get(args, :kill_timeout, @default_kill_timeout),
      register: Keyword.get(args, :register, :no_register)
    }

    case start(state) do
      {:ok, pid} ->
        unless state.register == :no_register do
          Elsa.Registry.register_name(state.register, pid)
        end

        {:ok, Map.put(state, :pid, pid)}

      {:error, reason} ->
        Logger.error(
          "#{__MODULE__}:#{inspect(self())} : wrapped process #{inspect(state.mfa)} failed to init for reason #{
            inspect(reason)
          }"
        )

        Process.sleep(state.delay)
        {:stop, reason}
    end
  end

  def handle_info({:EXIT, pid, reason}, %{pid: pid, delay: delay, started: started} = state) do
    lifetime = :erlang.system_time(:milli_seconds) - started

    max(delay - lifetime, 0)
    |> Process.sleep()

    {:stop, reason, state}
  end

  def handle_info(message, state) do
    Logger.info(
      "#{__MODULE__}:#{inspect(self())} : received invalid message #{inspect(message)}, current state: #{inspect(state)}"
    )

    {:noreply, state}
  end

  def terminate(reason, %{pid: pid} = state) do
    if Process.alive?(pid), do: kill(pid, reason, state.kill_timeout)

    reason
  end

  def terminate(reason, _state), do: reason

  defp kill(pid, reason, timeout) do
    Process.exit(pid, reason)

    receive do
      {:EXIT, ^pid, _reason} -> :ok
    after
      timeout -> Process.exit(pid, :kill)
    end
  end

  defp start(%{mfa: {module, function, args}}) do
    apply(module, function, args)
  end
end
