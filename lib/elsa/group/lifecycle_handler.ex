defmodule Elsa.Group.LifecycleHandler do
  alias Elsa.Group.Manager

  @callback assignment_received(Manager.group(), Manager.topic(), Manager.partition(), Manager.generation_id()) ::
              :ok | {:error, term()}

  @callback assignments_revoked() :: :ok

  defmacro __using__(_opts) do
    quote do
      @behaviour Elsa.Group.LifecycleHandler

      def assignment_received(_group, _topic, _partition, _generation_id) do
        :ok
      end

      def assignments_revoked() do
        :ok
      end

      defoverridable Elsa.Group.LifecycleHandler
    end
  end
end

defmodule Elsa.Group.DefaultLifecycleHandler do
  use Elsa.Group.LifecycleHandler
end
