# Copyright 2026 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

defmodule Mississippi.Consumer.Options.Test do
  use ExUnit.Case, async: true

  alias Mississippi.Consumer.DataUpdater.Handler.Impl
  alias Mississippi.Consumer.Options

  @schema Options.definition()

  test "an empty configuration returns defaults" do
    assert {:ok, opts} = NimbleOptions.validate([], @schema)
    assert opts[:mississippi_config][:message_handler] == Impl
    assert opts[:mississippi_config][:cluster_distribution_strategy] == :uniform_quorum
    assert is_integer(opts[:mississippi_config][:queues][:total_count])
  end

  test "allows setting valid distribution strategies" do
    valid_distribution_strategy = :uniform
    opts = [mississippi_config: [cluster_distribution_strategy: valid_distribution_strategy]]
    assert {:ok, res} = NimbleOptions.validate(opts, @schema)
    assert res[:mississippi_config][:cluster_distribution_strategy] == valid_distribution_strategy
  end

  test "does not allow invalid distribution strategies" do
    opts = [mississippi_config: [cluster_distribution_strategy: :not_a_distribution_strategy]]

    assert {:error, %{key: :cluster_distribution_strategy}} =
             NimbleOptions.validate(opts, @schema)
  end
end
