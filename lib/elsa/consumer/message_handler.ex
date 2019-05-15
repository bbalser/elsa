defmodule Elsa.Consumer.MessageHandler do
  @callback handle_message(term(), term()) :: :ack | :no_ack
  @callback handle_message(term()) :: :ack | :no_ack

  defmacro __using__(_opts) do
    quote do
      @behaviour Elsa.Consumer.MessageHandler

      def handle_message(message, state) do
        handle_message(message)
        {:ok, state}
      end

      def handle_message(message) do
        :ack
      end

      defoverridable Elsa.Consumer.MessageHandler
    end
  end

  def init(_group, %{handler: handler, init_args: init_args} = state) do
    {:ok, init_state} = apply(handler, :init, [init_args])
    {:ok, %{state | handler_state: init_state}}
  end

  def handle_message(topic, partition, message, state) do
    transformed_message = transform_message(topic, partition, message)
    {:ok, handler_state} = apply(state.handler, :handle_message, [transformed_message, state.handler_state])
    {:ok, %{state | handler_state: handler_state}}
  end

  defp transform_message(topic, partition, message) do
    {:kafka_message, offset, key, value, _, _, _} = message
    %{
      topic: topic,
      partition: partition,
      offset: offset,
      key: key,
      value: value
    }
  end

end
