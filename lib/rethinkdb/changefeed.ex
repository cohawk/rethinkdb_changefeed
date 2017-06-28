defmodule RethinkDB.Changefeed do
  @moduledoc """
  A behaviour for implementing RethinkDB change feeds.

  The `Changefeed` behaviour is a superset of the `GenServer` behaviour. It adds some return
  values and some callbacks to make handling updates simple.

  A very simple example Changefeed:

      defmodule PersonFeed do
        
        use RethinkDB.Changefeed

        def init(opts) do
          id = Keyword.get(opts, :id)
          db = Keyword.get(opts, :db)
          query = RethinkDB.Query.table("people")
            |> RethinkDB.Query.get(id)
            |> RethinkDB.Query.changes
          {:subscribe, query, db, nil}
        end

        def handle_update(%{"new_val" => data}, _) do
          {:next, data}
        end

        def handle_call(:get, _from, data) do
          {:reply, data, data}
        end
      end

  The example shows one of many patterns. In this case, we are keeping a local
  copy of the record and updating it whenever it changes in the database. Clients
  in the application can access the data via `Changefeed.call(pid, :get)`.

  The same pattern can be used on a sequence:

      defmodule TeamFeed do
        
        use RethinkDB.Changefeed

        def init(opts) do
          name = Keyword.get(opts, :name)
          team = Keyword.get(opts, :team) # team is a map of ids to maps
          db = Keyword.get(opts, :db)
          query = RethinkDB.Query.table("people")
            |> RethinkDB.Query.filter(%{team: name})
            |> RethinkDB.Query.changes
          {:subscribe, query, db, team}
        end

        def handle_update(data, team) do
          team = Enum.reduce(data, team, fn ->
            # no `old_val` means a new entry was created
            %{"new_val" => val, "old_val" => nil}, acc -> 
              Map.put(acc, val["id"], val)
            # no `new_val` means an entry was deleted
            %{"new_val" => nil, "old_val" => val}, acc -> 
              Map.delete(acc, val["id"])
            # otherwise, we have an update
            %{"new_val" => val}, acc ->
              Map.put(acc, val["id"], val)
          end)
          {:next, team}
        end

        def handle_call(:get, _from, data) do
          {:reply, data, data}
        end
      end

  A changefeed is designed to handle updates and to update any state associated with
  the feed. If a publisher subscriber model is desired, a GenEvent can be used in
  conjunction with a changefeed. Here's an example:

      defmodule EventFeed do
        
        use RethinkDB.Changefeed
        def init(opts) do
          gen_event = Keyword.get(opts, :gen_event)
          db = Keyword.get(opts, :db)
          query = RethinkDB.Query.table("events")
            |> RethinkDB.Query.changes
          {:subscribe, query, db, gen_event}
        end

        def handle_update(data, gen_event) do
          Enum.each(data, fn
            # no `old_val` means a new entry was created
            %{"new_val" => val, "old_val" => nil}, acc -> 
              GenEvent.notify(gen_event,{:create, val})
            # no `new_val` means an entry was deleted
            %{"new_val" => nil, "old_val" => val}, acc -> 
              GenEvent.notify(gen_event,{:delete, val})
            # otherwise, we have an update
            %{"new_val" => val, "old_val" => old_val}, acc ->
              GenEvent.notify(gen_event,{:update, old_val, val})
          end)
          {:next, gen_event}
        end
      end

  """

  use Connection

  require Logger

  defmacro __using__(_opts) do
    quote do
      @behaviour RethinkDB.Changefeed
    end
  end
  
  @doc """
    Called when process is first started. `start_link` blocks until init returns.

    Expects return to be one of the following:

    * `{:subscribe, query, db, state}` - Upon this returning, start_link will return
    and immediately a connection will be made to the database and a feed established.
    If a feed cannot be established then it will be retried with an exponential backoff.
    * `{:stop, reason}` - This will cause start_link to return `{:error, reason}` and the
    process will exit with reason `reason`

  """
  @callback init(opts :: any) :: any

  @doc """
    Called when new data is received from a feed.

    Expects return to be one of the following:

    * `{:next, state}` - Request the next set of data for the feed from the database.
    * `{:stop, reason, state}` - Stops the feed. `terminate/2` will be called with
    `reason` and `state`

  """
  @callback handle_update(update :: any, state :: any) :: any

  @doc """
    See `GenServer.handle_call/3`
  """
  @callback handle_call(request :: any, from :: any, state :: any) :: any

  @doc """
    See `GenServer.handle_cast/2`
  """
  @callback handle_cast(request :: any, state :: any) :: any


  @doc """
    See `GenServer.handle_info/2`
  """
  @callback handle_info(msg :: any, state :: any) :: any

  @doc """
    See `GenServer.code_change/3`
  """
  @callback code_change(vsn :: any, state :: any, extra :: any) :: any

  @doc """
    See `GenServer.terminate/2`
  """
  @callback terminate(reason :: any, state :: any) :: any

  @doc """
    See `GenServer.call/3`
  """
  defdelegate call(server, request, timeout), to: Connection 
  @doc """
    See `GenServer.call/2`
  """
  defdelegate call(server, request), to: Connection
  @doc """
    See `GenServer.cast/2`
  """
  defdelegate cast(server, request), to: Connection

  @doc """
    Start Changefeed process linked to current process.

    `args` will be passed into `init`. `opts` are standard GenServer options.
  """
  def start_link(mod, args, opts) do
    IO.inspect(mod, label: "INSPECT changefeed start_link/3 mod")
    IO.inspect(args, label: "INSPECT changefeed start_link/3 args")
    IO.inspect(opts, label: "INSPECT changefeed start_link/3 opts")
    Connection.start_link(__MODULE__, [mod: mod, args: args], opts)
  end

  def init(opts) do
    IO.inspect(opts, label: "INSPECT init/1 opts")
    mod = Keyword.get(opts, :mod)
    args = Keyword.get(opts, :args)
    {:subscribe, query, conn, feed_state} = mod.init(args)
    state = %{
      query: query,
      conn: conn,
      feed_state: feed_state,
      opts: opts,
      state: :connect
    }
    IO.inspect(state, label: "INSPECT changefeed init/1 state")
    {:connect, :init, state}
  end

  def connect(_info, state = %{query: query, conn: conn}) do
    IO.inspect(state, label: "INSPECT changefeed connect/2 state")
    case RethinkDB.run(query, conn, [timeout: :infinity]) do
      {:ok, msg = %RethinkDB.Feed{}} ->
        mod = get_in(state, [:opts, :mod])
        feed_state = Map.get(state, :feed_state)
        {:next, feed_state} = mod.handle_update(msg.data, feed_state)
        new_state = state
          |> Map.put(:task, next(msg))
          |> Map.put(:last, msg)
          |> Map.put(:feed_state, feed_state)
          |> Map.put(:state, :next)
        {:ok, new_state}
      {:ok, x} ->
        IO.puts("ERROR changefeed connect/2 X")
        Logger.debug(inspect x)
        IO.inspect(state, label: "INSPECT ERROR changefeed connect/2 state")
        backoff = min(Map.get(state, :timeout, 1000), 64000)
        IO.inspect(backoff, label: "INSPECT ERROR changefeed connect/2 backoff")
        {:backoff, backoff, Map.put(state, :timeout, backoff*2)}
      {:error, %{data: %{"r" => [error|_]}}} ->
        raise error
      {:error, %RethinkDB.Exception.ConnectionClosed{}} ->
        {:error, :connection_closed}
    end
  end

  def disconnect(_info, state = %{last: msg}) do
    RethinkDB.Connection.close(msg)
    {:stop, :normal, state}
  end

  def handle_call(msg, from, state) do
    mod = get_in(state, [:opts, :mod])
    feed_state = Map.get(state, :feed_state)
    case mod.handle_call(msg, from, feed_state) do
      {:reply, reply, new_feed_state} ->
        new_state = Map.put(state, :feed_state, new_feed_state)
        {:reply, reply, new_state}
      {:reply, reply, new_feed_state, timeout} ->
        new_state = Map.put(state, :feed_state, new_feed_state)
        {:reply, reply, new_state, timeout}
      {:noreply, new_feed_state} ->
        new_state = Map.put(state, :feed_state, new_feed_state)
        {:noreply, new_state}
      {:noreply, new_feed_state, timeout} ->
        new_state = Map.put(state, :feed_state, new_feed_state)
        {:noreply, new_state, timeout}
      {:stop, reason, reply, new_feed_state} ->
        new_state = Map.put(state, :feed_state, new_feed_state)
        {:stop, reason, reply, new_state}
      {:stop, reason, new_feed_state} ->
        new_state = Map.put(state, :feed_state, new_feed_state)
        {:stop, reason, new_state}
    end
  end

  def handle_cast(msg, state) do
    mod = get_in(state, [:opts, :mod])
    feed_state = Map.get(state, :feed_state)
    case mod.handle_cast(msg, feed_state) do
      {:noreply, new_feed_state} ->
        new_state = Map.put(state, :feed_state, new_feed_state)
        {:noreply, new_state}
      {:noreply, new_feed_state, timeout} ->
        new_state = Map.put(state, :feed_state, new_feed_state)
        {:noreply, new_state, timeout}
      {:stop, reason, new_feed_state} ->
        new_state = Map.put(state, :feed_state, new_feed_state)
        {:stop, reason, new_state}
    end
  end

  # TODO: handle_info pass through to callback. Look at Connection to see how they deal with it.
  ## TODO

  def handle_info({ref, msg}, state = %{state: :next, task: %Task{ref: ref}}) do
    Process.demonitor(ref, [:flush])
    case msg do
      %RethinkDB.Feed{data: data} ->
        mod = get_in(state, [:opts, :mod])
        feed_state = Map.get(state, :feed_state)
        {:next, feed_state} = mod.handle_update(data, feed_state)
        new_state = state
          |> Map.put(:task, next(msg))
          |> Map.put(:feed_state, feed_state)
          |> Map.put(:last, msg)
        {:noreply, new_state}
      _ ->
        # {:stop, :cohawk_rethinkdb_error, state}
        {:noreply, state}
    end
  end

  def handle_info(msg, state) do
    mod = get_in(state, [:opts, :mod])
    feed_state = Map.get(state, :feed_state)
    case mod.handle_info(msg, feed_state) do
      {:noreply, new_feed_state} ->
        new_state = Map.put(state, :feed_state, new_feed_state)
        {:noreply, new_state}
      {:noreply, new_feed_state, timeout} ->
        new_state = Map.put(state, :feed_state, new_feed_state)
        {:noreply, new_state, timeout}
      {:stop, reason, new_feed_state} ->
        new_state = Map.put(state, :feed_state, new_feed_state)
        {:stop, reason, new_state}
    end
  end

  def code_change(old_vsn, state, extra) do
    mod = get_in(state, [:opts, :mod])
    feed_state = Map.get(state, :feed_state)
    case mod.code_change(old_vsn, feed_state, extra) do
      {:ok, new_feed_state} -> {:ok, %{state | :feed_state => new_feed_state}}
      {:error, reason} -> {:error, reason}
    end
  end

  def terminate(reason, state) do
    IO.inspect(reason, label: "INSPECT changefeed terminate/2 reason")
    IO.inspect(state, label: "INSPECT changefeed terminate/2 state")
    mod = get_in(state, [:opts, :mod])
    IO.inspect(mod, label: "INSPECT changefeed terminate/2 mod")
    # feed_state = Keyword.get(state, :feed_state)
    feed_state = Map.get(state, :feed_state)
    IO.inspect(feed_state, label: "INSPECT changefeed terminate/2 feed_state")
    # mod.terminate(reason, feed_state)
  end

  defp next(f = %RethinkDB.Feed{}) do
    Task.async fn ->
      RethinkDB.next(f)
    end
  end
end
