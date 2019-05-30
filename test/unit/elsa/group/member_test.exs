defmodule Elsa.Group.MemberTest do
  use ExUnit.Case
  use Placebo
  import AsyncAssertion

  alias Elsa.Group

  describe "start_link/1" do
    setup do
      allow :brod_group_coordinator.start_link(any(), any(), any(), any(), any(), any()),
        return: {:ok, :group_coordinator_pid}

      :ok
    end

    test "starts group_coordinator" do
      {:ok, _pid} = Group.Member.start_link(name: :name, group: :group, topics: [:topic], config: [])

      assert_async fn ->
        assert_called :brod_group_coordinator.start_link(:name, :group, [:topic], [], Elsa.Group.Member, any())
      end
    end

    test ""
  end
end
