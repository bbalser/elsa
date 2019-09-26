defmodule Elsa.Group.DirectAcknowledgerTest do
  use ExUnit.Case
  use Placebo
  import AsyncAssertion

  alias Elsa.Group.DirectAcknowledger

  @client :brod_client
  @group "group1"
  @moduletag :capture_log

  setup do
    Process.flag(:trap_exit, true)
    :ok
  end

  describe "ack/6 - happy path" do
    setup do
      allow Elsa.Registry.whereis_name(any()), return: @client

      allow :brod_client.get_group_coordinator(any(), any()),
        return: {:ok, {:group_coordinator_endpoint, :group_coordinator_config}}

      allow :kpro.connect(any(), any()), return: {:ok, :connection}
      allow :brod_kafka_request.offset_commit(any(), any()), return: :offset_commit_kafka_request
      allow :brod_utils.request_sync(any(), any(), any()), return: {:ok, %{responses: []}}
      allow Elsa.Group.Consumer.ack(any(), any(), any(), any()), return: :ok

      {:ok, pid} = DirectAcknowledger.start_link(name: __MODULE__, client: @client, group: @group)
      on_exit(fn -> wait(pid) end)

      [pid: pid]
    end

    test "creates connection to group coordinator" do
      allow Elsa.Registry.whereis_name(any()), return: @client

      assert_async(fn ->
        assert_called :brod_client.get_group_coordinator(@client, @group)
        assert_called :kpro.connect(:group_coordinator_endpoint, :group_coordinator_config)
      end)
    end

    test "ack get sent to group coordinator connection", %{pid: pid} do
      member_id = :member_id
      topic = "topic1"
      partition = 0
      generation_id = 7
      offset = 32

      :ok = DirectAcknowledger.ack(pid, member_id, topic, partition, generation_id, offset)

      assert_called :brod_utils.request_sync(:connection, :offset_commit_kafka_request, 5_000)
    end
  end

  describe "ack/6 - exception paths" do
    test "dies when unable to find group_coordinator" do
      allow Elsa.Registry.whereis_name(any()), return: @client
      allow :brod_client.get_group_coordinator(any(), any()), return: {:error, :something_went_wrong}

      {:ok, pid} = DirectAcknowledger.start_link(name: __MODULE__, client: @client, group: @group)
      on_exit(fn -> wait(pid) end)

      assert_receive {:EXIT, ^pid, :something_went_wrong}
    end

    test "retries to connect to group coordinator when coordinator_not_available error" do
      allow Elsa.Registry.whereis_name(any()), return: @client

      allow :brod_client.get_group_coordinator(any(), any()),
        seq: [{:error, [error_code: :coordinator_not_available]}, {:error, :something_went_wrong}]

      {:ok, pid} = DirectAcknowledger.start_link(name: __MODULE__, client: @client, group: @group)
      on_exit(fn -> wait(pid) end)

      assert_receive {:EXIT, ^pid, :something_went_wrong}, 2_000

      assert_called :brod_client.get_group_coordinator(any(), any()), times(2)
    end

    test "dies when unable to connection to group coordinator" do
      allow Elsa.Registry.whereis_name(any()), return: @client

      allow :brod_client.get_group_coordinator(any(), any()),
        return: {:ok, {:group_coordinator_endpoint, :group_coordinator_config}}

      allow :kpro.connect(any(), any()), return: {:error, :bad_connection}

      {:ok, pid} = DirectAcknowledger.start_link(name: __MODULE__, client: @client, group: @group)
      on_exit(fn -> wait(pid) end)

      assert_receive {:EXIT, ^pid, :bad_connection}
    end
  end

  describe "bad responses from ack/6" do
    setup do
      allow Elsa.Registry.whereis_name(any()), return: @client

      allow :brod_client.get_group_coordinator(any(), any()),
        return: {:ok, {:group_coordinator_endpoint, :group_coordinator_config}}

      allow :kpro.connect(any(), any()), return: {:ok, :connection}
      allow :brod_kafka_request.offset_commit(any(), any()), return: :offset_commit_kafka_request

      {:ok, pid} = DirectAcknowledger.start_link(name: __MODULE__, client: @client, group: @group)
      on_exit(fn -> wait(pid) end)
      [pid: pid]
    end

    test "process dies when error in talking to coordinator", %{pid: pid} do
      allow :brod_utils.request_sync(any(), any(), any()), return: {:error, "some reason"}

      try do
        DirectAcknowledger.ack(pid, :member_id, "topic1", 0, 7, 32)
        flunk("Should have exited direct acknowledger")
      catch
        :exit, _ -> nil
      end

      assert_receive {:EXIT, ^pid, "some reason"}
    end

    test "process dies when ack response contains errors", %{pid: pid} do
      response = %{
        responses: [
          %{topic: "topic2", partition_responses: [%{error_code: :no_error, partition: 0}]},
          %{
            topic: "topic1",
            partition_responses: [%{error_code: :no_error, partition: 0}, %{error_code: :bad_stuff, partition: 1}]
          }
        ]
      }

      allow :brod_utils.request_sync(any(), any(), any()), return: {:ok, response}

      try do
        DirectAcknowledger.ack(pid, :member_id, "topic1", 0, 7, 32)
        flunk("Should have exited direct acknowledger")
      catch
        :exit, _ -> nil
      end

      assert_receive {:EXIT, ^pid, [%{topic: "topic1", partition: 1, error: :bad_stuff}]}
    end
  end

  defp wait(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
