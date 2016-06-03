%% Copyright (c) 2009 
%% Jacob Vorreuter <jacob.vorreuter@gmail.com>
%%
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%%
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
%%
%% http://code.google.com/p/memcached/wiki/MemcacheBinaryProtocol
%% @doc a binary protocol memcached client
-module(erlmc_conn).
-behaviour(gen_server).

-include("erlmc.hrl").

%% gen_server callbacks
-export([start_link/1, start_link/4, init/1, handle_call/3, handle_cast/2, 
	     handle_info/2, terminate/2, code_change/3]).

-record(state, {socket, recv_timeout}).

%% API functions
start_link([Host, Port]) ->
    start_link([Host, Port], 1000, 500, 500).

start_link([Host, Port], ConnectTimeout, SendTimeout, RecvTimeout) ->
	gen_server:start_link(?MODULE, [Host, Port, ConnectTimeout, SendTimeout, RecvTimeout], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%% @hidden
%%--------------------------------------------------------------------
init([Host, Port, ConnectTimeout, SendTimeout, RecvTimeout]) ->
    case gen_tcp:connect(Host, Port, [binary,
                                      {packet, 0},
                                      {active, false},
                                      {send_timeout, SendTimeout}], ConnectTimeout) of
        {ok, Socket} -> 
			{ok, #state{socket=Socket, recv_timeout=RecvTimeout}};
        Error ->
			exit(Error)
    end.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%% @hidden
%%--------------------------------------------------------------------    
handle_call({get, Key}, _From, State) ->
    case send_recv(State, #request{op_code=?OP_GetK, key=list_to_binary(Key)}) of
		{error, Err} ->
			{stop, Err, {error, Err}, State};
		#response{key=Key1, value=Value} ->
    		case binary_to_list(Key1) of
		        Key -> {reply, Value, State};
		        _ -> {reply, <<>>, State}
		    end
	end;
    
handle_call({add, Key, Value, Expiration}, _From, State) ->
    case send_recv(State, #request{op_code=?OP_Add, extras = <<16#deadbeef:32, Expiration:32>>, key=list_to_binary(Key), value=Value}) of
		{error, Err} ->
			{stop, Err, {error, Err}, State};
		Resp ->
    		{reply, Resp#response.value, State}
	end;
    
handle_call({set, Key, Value, Expiration}, _From, State) ->
	case send_recv(State, #request{op_code=?OP_Set, extras = <<16#deadbeef:32, Expiration:32>>, key=list_to_binary(Key), value=Value}) of
		{error, Err} ->
			{stop, Err, {error, Err}, State};
		Resp ->
    		{reply, Resp#response.value, State}
	end;

handle_call({replace, Key, Value, Expiration}, _From, State) ->
	case send_recv(State, #request{op_code=?OP_Replace, extras = <<16#deadbeef:32, Expiration:32>>, key=list_to_binary(Key), value=Value}) of
		{error, Err} ->
			{stop, Err, {error, Err}, State};
		Resp ->
    		{reply, Resp#response.value, State}
	end;

handle_call({delete, Key}, _From, State) ->
	case send_recv(State, #request{op_code=?OP_Delete, key=list_to_binary(Key)}) of
		{error, Err} ->
			{stop, Err, {error, Err}, State};
		Resp ->
    		{reply, Resp#response.value, State}
	end;

handle_call({increment, Key, Value, Initial, Expiration}, _From, State) ->
	case send_recv(State, #request{op_code=?OP_Increment, extras = <<Value:64, Initial:64, Expiration:32>>, key=list_to_binary(Key)}) of
		{error, Err} ->
			{stop, Err, {error, Err}, State};
		Resp ->
    		{reply, Resp#response.value, State}
	end;
	
handle_call({decrement, Key, Value, Initial, Expiration}, _From, State) ->
	case send_recv(State, #request{op_code=?OP_Decrement, extras = <<Value:64, Initial:64, Expiration:32>>, key=list_to_binary(Key)}) of
		{error, Err} ->
			{stop, Err, {error, Err}, State};
		Resp ->
    		{reply, Resp#response.value, State}
	end;

handle_call({append, Key, Value}, _From, State) ->
	case send_recv(State, #request{op_code=?OP_Append, key=list_to_binary(Key), value=Value}) of
		{error, Err} ->
			{stop, Err, {error, Err}, State};
		Resp ->
    		{reply, Resp#response.value, State}
	end;

handle_call({prepend, Key, Value}, _From, State) ->
	case send_recv(State, #request{op_code=?OP_Prepend, key=list_to_binary(Key), value=Value}) of
		{error, Err} ->
			{stop, Err, {error, Err}, State};
		Resp ->
    		{reply, Resp#response.value, State}
	end;
	
handle_call(stats, _From, State) ->
	send(State, #request{op_code=?OP_Stat}),
    case collect_stats_from_socket(State) of
		{error, Err} ->
			{stop, Err, {error, Err}, State};
		Reply ->
    		{reply, Reply, State}
	end;

handle_call(flush, _From, State) ->
	case send_recv(State, #request{op_code=?OP_Flush}) of
		{error, Err} ->
			{stop, Err, {error, Err}, State};
		Resp ->
    		{reply, Resp#response.value, State}
	end;
        
handle_call({flush, Expiration}, _From, State) ->
	case send_recv(State, #request{op_code=?OP_Flush, extras = <<Expiration:32>>}) of
		{error, Err} ->
			{stop, Err, {error, Err}, State};
		Resp ->
    		{reply, Resp#response.value, State}
	end;
    
handle_call(quit, _From, #state{socket=Socket} = State) ->
	send_recv(State, #request{op_code=?OP_Quit}),
	gen_tcp:close(Socket),
    {stop, shutdown, undefined};
    
handle_call(version, _From, State) ->
	case send_recv(State, #request{op_code=?OP_Version}) of
		{error, Err} ->
			{stop, Err, {error, Err}, State};
		Resp ->
    		{reply, Resp#response.value, State}
	end;
	
handle_call(_, _From, State) -> {reply, {error, invalid_call}, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%% @hidden
%%--------------------------------------------------------------------
handle_cast(_Message, State) -> {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%% @hidden
%%--------------------------------------------------------------------
handle_info(_Info, State) -> {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% @hidden
%%--------------------------------------------------------------------
terminate(shutdown, undefined) ->
    ok;
terminate(_Reason, #state{socket=Socket}) -> 
	case is_port(Socket) of
		true -> gen_tcp:close(Socket);
		false -> ok
	end, ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%% @hidden
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------     
collect_stats_from_socket(State) ->
    collect_stats_from_socket(State, []).
    
collect_stats_from_socket(State, Acc) ->
    case recv(State) of
		{error, Err} -> 
			{error, Err};
        #response{body_size=0} ->
            Acc;
        #response{key=Key, value=Value} ->
            collect_stats_from_socket(State, [{binary_to_atom(Key, utf8), binary_to_list(Value)}|Acc])
    end.

send_recv(State, Request) ->
    ok = send(State, Request),
    recv(State).
    
send(#state{socket=Socket}, Request) ->
    Bin = encode_request(Request),
    gen_tcp:send(Socket, Bin).

recv(State) ->
    case recv_header(State) of
		{error, Err} ->
			{error, Err};
		HdrResp ->
    		recv_body(State, HdrResp)
    end.
        
encode_request(Request) when is_record(Request, request) ->
    Magic = 16#80,
    Opcode = Request#request.op_code,
    KeySize = size(Request#request.key),
    Extras = Request#request.extras,
    ExtrasSize = size(Extras),
    DataType = Request#request.data_type,
    Reserved = Request#request.reserved,
    Body = <<Extras:ExtrasSize/binary, (Request#request.key)/binary, (Request#request.value)/binary>>,
    BodySize = size(Body),
    Opaque = Request#request.opaque,
    CAS = Request#request.cas,
    <<Magic:8, Opcode:8, KeySize:16, ExtrasSize:8, DataType:8, Reserved:16, BodySize:32, Opaque:32, CAS:64, Body:BodySize/binary>>.

recv_header(State) ->
    decode_response_header(recv_bytes(State, 24)).
  
recv_body(State, #response{key_size = KeySize, extras_size = ExtrasSize, body_size = BodySize}=Resp) ->
    decode_response_body(recv_bytes(State, BodySize), ExtrasSize, KeySize, Resp).
    
decode_response_header({error, Err}) -> {error, Err};
decode_response_header(<<16#81:8, Opcode:8, KeySize:16, ExtrasSize:8, DataType:8, Status:16, BodySize:32, Opaque:32, CAS:64>>) ->
    #response{
        op_code = Opcode, 
        data_type = DataType, 
        status = Status, 
        opaque = Opaque, 
        cas = CAS, 
        key_size = KeySize,
        extras_size = ExtrasSize,
        body_size = BodySize
    }.
    
decode_response_body({error, Err}, _, _, _) -> {error, Err};
decode_response_body(Bin, ExtrasSize, KeySize, Resp) ->
    <<Extras:ExtrasSize/binary, Key:KeySize/binary, Value/binary>> = Bin,
    Resp#response{
        extras = Extras,
        key = Key,
        value = Value
    }.

recv_bytes(_, 0) -> <<>>;
recv_bytes(#state{socket=Socket, recv_timeout=RecvTimeout}, NumBytes) ->
    case gen_tcp:recv(Socket, NumBytes, RecvTimeout) of
        {ok, Bin} -> Bin;
        {error, Reason} = Err ->
            %% If we encounter ANY error when receiving data on the socket, close the
            %% socket and kill this connection process.  The thinking behind this is:
            %% let's say we hit a read timeout but the socket stays open.  What happens
            %% to the next request?  Is it possible it could read the previous request's
            %% response?  The memcached protocol doesn't seem to include any sort of
            %% "request id," so I'm not sure.  I *am* sure that I don't want the wrong
            %% response...ever!  Best way to ensure that is to close the socket down.
            error_logger:error_msg("erlmc_conn is closing and exiting to prevent stale read, reason: ~p~n", [Reason]),
            gen_tcp:close(Socket),
            exit(Reason),
            Err
    end.
