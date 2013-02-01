-module(efluent).
-export([start_link/0, start_link/1, start_link/2, start_link/3]).
-export([send/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-record(state, {
        socket,
        tag,
        host,
        port,
        timeout
    }).

get_unix_timestamp() ->
    get_unix_timestamp(os:timestamp()).

get_unix_timestamp({Mega, Sec, _Micro}) ->
    (Mega * 1000000) + Sec.

start_link() ->
    start_link("debug").

start_link(Tag) ->
    start_link(Tag, {127, 0, 0, 1}).

start_link(Tag, Host) ->
    start_link(Tag, Host, 24224).

start_link(Tag, Host, Port) ->
    start_link(Tag, Host, Port, 3000).

start_link(Tag, Host, Port, Timeout) ->
    gen_server:start_link(?MODULE, [Tag, Host, Port, Timeout], []).

send(Pid, Label, Message) ->
    gen_server:cast(Pid, {send, iolist_to_binary(Label), iolist_to_binary(Message)}).

init([Tag, Host, Port, Timeout]) ->
    {ok, Socket} = gen_tcp:connect(Host, Port, [{active, true}], Timeout), 
    {ok, #state{
            socket  = Socket,
            tag     = Tag,
            host    = Host,
            port    = Port,
            timeout = Timeout
        }}.

handle_call(_Call, _From, State) ->
    {reply, undefined, State}.

handle_cast({send, Label, Message}, State = #state{socket = Socket, tag = Tag}) ->
    Time    = get_unix_timestamp(),
    MsgTag  = iolist_to_binary([Tag, $., Label]),
    Packet  = msgpack:pack([MsgTag, Time, Message]),
    gen_tcp:send(Socket, Packet),
    {noreply, State}.

handle_info({tcp_closed, _OldSocket}, State = #state{host = Host, port = Port, timeout = Timeout}) ->
    {ok, NewSocket} = gen_tcp:connect(Host, Port, [{active, true}], Timeout),
    {noreply, State#state{socket = NewSocket}};
handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
