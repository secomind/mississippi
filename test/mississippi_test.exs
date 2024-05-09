defmodule MississippiTest do
  use ExUnit.Case
  doctest Mississippi

  test "greets the world" do
    assert Mississippi.hello() == :world
  end
end
