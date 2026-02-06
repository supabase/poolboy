%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy_top_sup).
-behaviour(supervisor).

-export([start_link/2, init/1]).

start_link(PoolArgs, WorkerArgs) ->
    case proplists:get_value(name, PoolArgs) of
        undefined ->
            {error, {missing_option, name}};
        _ ->
            supervisor:start_link(?MODULE, {PoolArgs, WorkerArgs})
    end.

init({PoolArgs, WorkerArgs}) ->
    Mod = proplists:get_value(worker_module, PoolArgs),
    Name = proplists:get_value(name, PoolArgs),
    {ok, {{one_for_all, 0, 1}, [
                #{id => poolboy_sup,
                  start => {poolboy_sup, start_link, [Mod, WorkerArgs]},
                  restart => permanent,
                  shutdown => infinity,
                  type => supervisor,
                  modules => [poolboy_sup]},
                #{id => poolboy,
                  start => {poolboy, start_link_worker, [Name, PoolArgs]},
                  restart => permanent,
                  shutdown => 5000,
                  type => worker,
                  modules => [poolboy]}
            ]}}.
