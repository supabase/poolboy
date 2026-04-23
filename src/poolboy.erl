%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy).
-behaviour(gen_server).

-export([checkout/1, checkout/2, checkout/3, checkin/2, transaction/2,
         transaction/3, child_spec/2, child_spec/3,
         start_link/2, start_link_worker/2, stop/1, status/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         handle_continue/2, terminate/2, code_change/3]).
-export_type([pool/0]).

-define(DEFAULT_TIMEOUT, 5000).

-type pid_queue() :: queue:queue().

-type pool() ::
    Name :: (atom() | pid()) |
    {Name :: atom(), node()} |
    {local, Name :: atom()} |
    {global, GlobalName :: any()} |
    {via, Module :: atom(), ViaName :: any()}.

% Copied from gen:start_ret/0
-type start_ret() :: {'ok', pid()} | 'ignore' | {'error', term()}.

-record(state, {
    supervisor :: undefined | pid(),
    workers :: undefined | pid_queue(),
    waiting :: pid_queue(),
    monitors :: ets:tid(),
    size = 5 :: non_neg_integer(),
    overflow = 0 :: non_neg_integer(),
    max_overflow = 10 :: non_neg_integer(),
    strategy = lifo :: lifo | fifo,
    idle_workers = #{} :: map(),
    idle_timeout = timer:minutes(5) :: non_neg_integer(),
    reclaim_strategy = checkin :: checkin | kill
}).

-spec checkout(Pool :: pool()) -> pid().
checkout(Pool) ->
    checkout(Pool, true).

-spec checkout(Pool :: pool(), Block :: boolean()) -> pid() | full.
checkout(Pool, Block) ->
    checkout(Pool, Block, ?DEFAULT_TIMEOUT).

-spec checkout(Pool :: pool(), Block :: boolean(), Timeout :: timeout())
    -> pid() | full.
checkout(Pool, Block, Timeout) ->
    CRef = make_ref(),
    try
        gen_server:call(Pool, {checkout, CRef, Block}, Timeout)
    catch
        Class:Reason:Stacktrace ->
            gen_server:cast(Pool, {cancel_waiting, CRef}),
            erlang:raise(Class, Reason, Stacktrace)
    end.

-spec checkin(Pool :: pool(), Worker :: pid()) -> ok.
checkin(Pool, Worker) when is_pid(Worker) ->
    gen_server:cast(Pool, {checkin, Worker}).

-spec transaction(Pool :: pool(), Fun :: fun((Worker :: pid()) -> any()))
    -> any().
transaction(Pool, Fun) ->
    transaction(Pool, Fun, ?DEFAULT_TIMEOUT).

-spec transaction(Pool :: pool(), Fun :: fun((Worker :: pid()) -> any()),
    Timeout :: timeout()) -> any().
transaction(Pool, Fun, Timeout) ->
    Worker = poolboy:checkout(Pool, true, Timeout),
    try
        Fun(Worker)
    after
        ok = poolboy:checkin(Pool, Worker)
    end.

-spec child_spec(PoolId :: term(), PoolArgs :: proplists:proplist())
    -> supervisor:child_spec().
child_spec(PoolId, PoolArgs) ->
    child_spec(PoolId, PoolArgs, []).

-spec child_spec(PoolId :: term(),
                 PoolArgs :: proplists:proplist(),
                 WorkerArgs :: proplists:proplist())
    -> supervisor:child_spec().
child_spec(PoolId, PoolArgs, WorkerArgs) ->
    #{id => PoolId,
      start => {poolboy, start_link, [PoolArgs, WorkerArgs]},
      restart => permanent,
      shutdown => infinity,
      type => supervisor,
      modules => [poolboy, poolboy_sup, poolboy_top_sup]}.

-spec start_link(PoolArgs :: proplists:proplist(),
                 WorkerArgs:: proplists:proplist())
    -> start_ret().
start_link(PoolArgs, WorkerArgs)  ->
    poolboy_top_sup:start_link(PoolArgs, WorkerArgs).

-spec start_link_worker(Name :: pool(), PoolArgs :: proplists:proplist())
    -> start_ret().
start_link_worker(Name, PoolArgs) ->
    gen_server:start_link(Name, ?MODULE, PoolArgs, []).

-spec stop(Pool :: pool()) -> ok.
stop(Pool) ->
    {ok, SupPid} = gen_server:call(Pool, get_top_sup),
    gen_server:stop(SupPid).

-spec status(Pool :: pool()) -> {atom(), integer(), integer(), integer()}.
status(Pool) ->
    gen_server:call(Pool, status).

init(PoolArgs) ->
    process_flag(trap_exit, true),
    Waiting = queue:new(),
    Monitors = ets:new(monitors, [private]),
    State = parse_opts(PoolArgs, #state{waiting = Waiting, monitors = Monitors}),
    {ok, State, {continue, init}}.

handle_continue(init, State) ->
    #state{size = Size} = State,
    Sup = find_worker_sup(),
    Workers = prepopulate(Size, Sup),
    {noreply, State#state{supervisor = Sup, workers = Workers}}.

handle_cast({checkin, Pid}, State = #state{monitors = Monitors}) ->
    case ets:lookup(Monitors, Pid) of
        [{Pid, _, MRef}] ->
            true = erlang:demonitor(MRef),
            true = ets:delete(Monitors, Pid),
            NewState = handle_checkin(Pid, State),
            {noreply, NewState};
        [] ->
            {noreply, State}
    end;

handle_cast({cancel_waiting, CRef}, State) ->
    case ets:match(State#state.monitors, {'$1', CRef, '$2'}) of
        [[Pid, MRef]] ->
            demonitor(MRef, [flush]),
            true = ets:delete(State#state.monitors, Pid),
            NewState = handle_checkin(Pid, State),
            {noreply, NewState};
        [] ->
            Cancel = fun({_, Ref, MRef}) when Ref =:= CRef ->
                             demonitor(MRef, [flush]),
                             false;
                        (_) ->
                             true
                     end,
            Waiting = queue:filter(Cancel, State#state.waiting),
            {noreply, State#state{waiting = Waiting}}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call({checkout, CRef, Block}, {FromPid, _} = From, State) ->
    #state{supervisor = Sup,
           workers = Workers,
           monitors = Monitors,
           overflow = Overflow,
           max_overflow = MaxOverflow,
           idle_workers = IdleWorkers,
           strategy = Strategy} = State,
    case get_worker_with_strategy(Workers, Strategy) of
        {{value, Pid},  Left} ->
            {NewIdleWorkers, NewOverflow} =
                case maps:get(Pid, IdleWorkers, undefined) of
                    undefined ->
                        {IdleWorkers, Overflow};
                    Timer ->
                        erlang:cancel_timer(Timer),
                        {maps:remove(Pid, IdleWorkers), Overflow + 1}
                end,
            MRef = erlang:monitor(process, FromPid),
            true = ets:insert(Monitors, {Pid, CRef, MRef}),
            {reply, Pid, State#state{workers = Left, idle_workers = NewIdleWorkers, overflow = NewOverflow}};
        {empty, _Left} when MaxOverflow > 0, Overflow + map_size(IdleWorkers) < MaxOverflow ->
            {Pid, MRef} = new_worker(Sup, FromPid),
            true = ets:insert(Monitors, {Pid, CRef, MRef}),
            {reply, Pid, State#state{overflow = Overflow + 1}};
        {empty, _Left} when Block =:= false ->
            {reply, full, State};
        {empty, _Left} ->
            MRef = erlang:monitor(process, FromPid),
            Waiting = queue:in({From, CRef, MRef}, State#state.waiting),
            {noreply, State#state{waiting = Waiting}}
    end;

handle_call(status, _From, State) ->
    #state{workers = Workers,
           monitors = Monitors,
           overflow = Overflow} = State,
    StateName = state_name(State),
    {reply, {StateName, queue:len(Workers), Overflow, ets:info(Monitors, size)}, State};
handle_call(get_avail_workers, _From, State) ->
    Workers = State#state.workers,
    {reply, Workers, State};
handle_call(get_all_workers, _From, State) ->
    Sup = State#state.supervisor,
    WorkerList = supervisor:which_children(Sup),
    {reply, WorkerList, State};
handle_call(get_idle_workers, _From, State) ->
    Workers = State#state.idle_workers,
    {reply, Workers, State};
handle_call(get_all_monitors, _From, State) ->
    Monitors = ets:select(State#state.monitors,
                          [{{'$1', '_', '$2'}, [], [{{'$1', '$2'}}]}]),
    {reply, Monitors, State};
handle_call(get_top_sup, _From, State) ->
    [Parent | _] = get('$ancestors'),
    {reply, {ok, Parent}, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Msg, _From, State) ->
    Reply = {error, invalid_message},
    {reply, Reply, State}.

handle_info({'DOWN', MRef, _, _, _}, State) ->
    case ets:match(State#state.monitors, {'$1', '_', MRef}) of
        [[Pid]] ->
            true = ets:delete(State#state.monitors, Pid),
            NewState = handle_owner_down(Pid, State),
            {noreply, NewState};
        [] ->
            Waiting = queue:filter(fun ({_, _, R}) -> R =/= MRef end, State#state.waiting),
            {noreply, State#state{waiting = Waiting}}
    end;
handle_info({'EXIT', Pid, _Reason}, State) ->
    #state{supervisor = Sup,
           monitors = Monitors} = State,
    case ets:lookup(Monitors, Pid) of
        [{Pid, _, MRef}] ->
            true = erlang:demonitor(MRef),
            true = ets:delete(Monitors, Pid),
            NewState = handle_worker_exit(Pid, State),
            {noreply, NewState};
        [] ->
            WasIdle = maps:is_key(Pid, State#state.idle_workers),
            W = filter_worker_by_pid(Pid, State#state.workers),
            % if it was idle, don't restart
            case WasIdle of
                true ->
                    I = remove_from_idle(Pid, State#state.idle_workers),
                    {noreply, State#state{workers = W, idle_workers = I}};
                false ->
                    {noreply, State#state{workers = queue:in(new_worker(Sup), W)}}
            end
    end;

handle_info({dismiss_idle, Pid}, #state{supervisor = Sup, idle_workers = IdleWorkers} = State) ->
    ok = dismiss_worker(Sup, Pid),
    NewIdleWorkers = maps:remove(Pid, IdleWorkers),
    Workers = filter_worker_by_pid(Pid, State#state.workers),
    NewState = State#state{idle_workers = NewIdleWorkers, workers = Workers},
    {noreply, NewState};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    Workers = queue:to_list(State#state.workers),
    ok = lists:foreach(fun (W) -> unlink(W) end, Workers),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

parse_opts([{size, Size} | Rest], State) when is_integer(Size) ->
    parse_opts(Rest, State#state{size = Size});
parse_opts([{max_overflow, MaxOverflow} | Rest], State) when is_integer(MaxOverflow) ->
    parse_opts(Rest, State#state{max_overflow = MaxOverflow});
parse_opts([{idle_timeout, IdleTimeout} | Rest], State) when is_integer(IdleTimeout) ->
    parse_opts(Rest, State#state{idle_timeout = IdleTimeout});
parse_opts([{strategy, lifo} | Rest], State) ->
    parse_opts(Rest, State#state{strategy = lifo});
parse_opts([{strategy, fifo} | Rest], State) ->
    parse_opts(Rest, State#state{strategy = fifo});
parse_opts([{reclaim_strategy, checkin} | Rest], State) ->
    parse_opts(Rest, State#state{reclaim_strategy = checkin});
parse_opts([{reclaim_strategy, kill} | Rest], State) ->
    parse_opts(Rest, State#state{reclaim_strategy = kill});
parse_opts([_ | Rest], State) ->
    parse_opts(Rest, State);
parse_opts([], State) ->
    State.

find_worker_sup() ->
    [Parent | _] = get('$ancestors'),
    Children = supervisor:which_children(Parent),
    {poolboy_sup, Pid, _, _} = lists:keyfind(poolboy_sup, 1, Children),
    Pid.

new_worker(Sup) ->
    {ok, Pid} = supervisor:start_child(Sup, []),
    true = link(Pid),
    Pid.

new_worker(Sup, FromPid) ->
    Pid = new_worker(Sup),
    Ref = erlang:monitor(process, FromPid),
    {Pid, Ref}.

get_worker_with_strategy(Workers, fifo) ->
    queue:out(Workers);
get_worker_with_strategy(Workers, lifo) ->
    queue:out_r(Workers).

dismiss_worker(Sup, Pid) ->
    true = unlink(Pid),
    supervisor:terminate_child(Sup, Pid).

filter_worker_by_pid(Pid, Workers) ->
    queue:filter(fun (WPid) -> WPid =/= Pid end, Workers).

prepopulate(N, _Sup) when N < 1 ->
    queue:new();
prepopulate(N, Sup) ->
    prepopulate(N, Sup, queue:new()).

prepopulate(0, _Sup, Workers) ->
    Workers;
prepopulate(N, Sup, Workers) ->
    prepopulate(N-1, Sup, queue:in(new_worker(Sup), Workers)).

handle_checkin(Pid, State) ->
    #state{waiting = Waiting,
           monitors = Monitors,
           idle_workers = IdleWorkers,
           overflow = Overflow} = State,
    case queue:out(Waiting) of
        {{value, {From, CRef, MRef}}, Left} ->
            true = ets:insert(Monitors, {Pid, CRef, MRef}),
            gen_server:reply(From, Pid),
            State#state{waiting = Left};
        {empty, Empty} when Overflow > 0 ->
            Timer = erlang:send_after(State#state.idle_timeout, self(), {dismiss_idle, Pid}),
            NewIdleWorkers = maps:put(Pid, Timer, IdleWorkers),
            Workers = queue:in(Pid, State#state.workers),
            State#state{workers = Workers, waiting = Empty, overflow = Overflow - 1, idle_workers = NewIdleWorkers};
        {empty, Empty} ->
            Workers = queue:in(Pid, State#state.workers),
            State#state{workers = Workers, waiting = Empty, overflow = 0}
    end.

handle_owner_down(Pid, #state{reclaim_strategy = checkin} = State) ->
    handle_checkin(Pid, State);
handle_owner_down(Pid, #state{reclaim_strategy = kill, supervisor = Sup} = State) ->
    ok = dismiss_worker(Sup, Pid),
    handle_worker_exit(Pid, State).

handle_worker_exit(Pid, State) ->
    #state{supervisor = Sup,
           monitors = Monitors,
           idle_workers = IdleWorkers,
           overflow = Overflow} = State,
    NewIdleWorkers = remove_from_idle(Pid, IdleWorkers),
    case queue:out(State#state.waiting) of
        {{value, {From, CRef, MRef}}, LeftWaiting} ->
            NewWorker = new_worker(State#state.supervisor),
            true = ets:insert(Monitors, {NewWorker, CRef, MRef}),
            gen_server:reply(From, NewWorker),
            State#state{waiting = LeftWaiting, idle_workers = NewIdleWorkers};
        {empty, Empty} when Overflow > 0 ->
            State#state{overflow = Overflow - 1, waiting = Empty, idle_workers = NewIdleWorkers};
        {empty, Empty} ->
            W = filter_worker_by_pid(Pid, State#state.workers),
            Workers = queue:in(new_worker(Sup), W),
            State#state{workers = Workers, waiting = Empty, idle_workers = NewIdleWorkers}
    end.

state_name(State = #state{overflow = Overflow}) when Overflow < 1 ->
    #state{max_overflow = MaxOverflow, workers = Workers} = State,
    case queue:len(Workers) == 0 of
        true when MaxOverflow < 1 -> full;
        true -> overflow;
        false -> ready
    end;
state_name(#state{overflow = MaxOverflow, max_overflow = MaxOverflow}) ->
    full;
state_name(_State) ->
    overflow.

remove_from_idle(Pid, IdleWorkers) ->
    case maps:take(Pid, IdleWorkers) of
        error ->
            IdleWorkers;
        {Timer, NewIdleWorkers} ->
            erlang:cancel_timer(Timer),
            NewIdleWorkers
    end.
