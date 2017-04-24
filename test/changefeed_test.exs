defmodule ChangesTest do
  use ExUnit.Case, async: false
  use RethinkDB.Connection
  import RethinkDB.Query

  @table_name "changes_test_table_1"


  #test supervisable changefeed

  defmodule TestChangefeed do
    use RethinkDB.Changefeed

    require Logger

    def init({q, pid}) do
      IO.inspect(pid, label: "init/2 pid")
      {:subscribe, q, ChangesTest, {:setup, pid}}
    end
    def init({q, pid, db}) do
      IO.inspect(pid, label: "init/3 pid")
      IO.inspect(db, label: "init/3 db")
      {:subscribe, q, db, {:setup, pid}}
    end

    def handle_update(foo, {:setup, pid}) do
      IO.inspect(pid, label: "handle_update(foo, {:setup, pid}")
      send pid, {:ready, foo}
      {:next, pid}
    end

    def handle_update(foo, pid) do
      send pid, {:update, foo}
      {:next, pid}
    end

    def handle_call(:ping, _from, pid) do
      {:reply, {:pong, pid}, pid}
    end

    def handle_cast({:ping, p}, state) do
      send p, :pong
      {:noreply, state}
    end

    def handle_info({:ping, p}, state) do
      send p, :pong
      {:noreply, state}
    end

    def handle_info(:stop, state) do
      {:stop, :normal, state}
    end

    def terminate(_, pid) do
      send pid, :terminated
    end

    def code_change(_, pid, _) do
      send pid, :change
      {:ok, pid}
    end
  end


  test "single document changefeed" do
    # %RethinkDB.Record{data: %{"generated_keys" => [id]}} = table(@table_name)
    #                      |> insert(%{"test" => "value"}) |> run
    id = "37642a8d-26be-4fbe-a6c1-bc082b7c8069"
    q = table(@table_name) |> get(id) |> changes(include_initial: true)
    {:ok, _} = RethinkDB.Changefeed.start_link(
      TestChangefeed,
      {q,self()},
      [])
    receive do
      {:ready, data} ->
        assert data["new_val"]["test"] == "value"
    end
    table(@table_name) |> get(id) |> update(%{"test" => "new_value"}) |> run
    receive do
      {:update, data} ->
        assert data["new_val"]["test"] == "new_value"
    end
  end

end
