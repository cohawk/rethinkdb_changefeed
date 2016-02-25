# `RethinkDB.Changefeed`

Supervised changefeeds for RethinkDB and Elixir. This package was previously a part of the
[RethinkDB driver](https://github.com/hamiltop/rethinkdb_changefeed). It was broken out
into a separate package to enable an independent release cycle.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed as:

  1. Add rethinkdb_changefeed to your list of dependencies in `mix.exs`:

        def deps do
          [{:rethinkdb_changefeed, "~> 0.0.1"}]
        end

  2. Ensure rethinkdb_changefeed is started before your application:

        def application do
          [applications: [:rethinkdb_changefeed]]
        end

