defmodule Elsa.Wrapper do
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
      delay: Keyword.get(args, :delay, @default_delay),
      kill_timeout: Keyword.get(args, :kill_timeout, @default_kill_timeout),
      register: Keyword.get(args, :register, :no_register)
    }

    {:ok, pid} = start(state)

    unless state.register == :no_register do
      Elsa.Registry.register_name(state.register, pid)
    end

    {:ok, Map.put(state, :pid, pid)}
  end

  def handle_info({:EXIT, pid, reason}, %{pid: pid, delay: delay} = state) do
    Process.sleep(delay)
    {:stop, reason, state}
  end

  def handle_info(message, state) do
    Logger.info(
      "#{__MODULE__}:#{inspect(self())} : received invalid message #{inspect(message)}, current state: #{inspect(state)}"
    )

    {:noreply, state}
  end

  def terminate(reason, %{pid: pid} = state) do
    case Process.alive?(pid) do
      true -> kill(pid, reason, state.kill_timeout)
      false -> :ok
    end

    reason
  end

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
