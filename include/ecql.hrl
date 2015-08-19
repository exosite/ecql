%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql.hrl - Header
%%==============================================================================

%% Records
-record(frame, {stream, opcode, body}).

%% Defines
-define(VS_REQUEST, 2).
-define(VS_RESPONSE, 130).
-define(VS_RESPONSE3, 131).

-define(OP_ERROR, 0).
-define(OP_STARTUP, 1).
-define(OP_READY, 2).
-define(OP_AUTHENTICATE, 3).
-define(OP_QUERY, 7).
-define(OP_RESULT, 8).
-define(OP_PREPARE, 9).
-define(OP_EXECUTE, 10).
-define(OP_BATCH, 13).
-define(OP_AUTH_CHALLENGE, 14).
-define(OP_AUTH_RESPONSE, 15).
-define(OP_AUTH_SUCCESS, 16).

-define(T_UINT8,   8/big-unsigned-integer).
-define(T_UINT16, 16/big-unsigned-integer).
-define(T_UINT32, 32/big-unsigned-integer).
-define(T_INT8,    8/big-signed-integer).
-define(T_INT16,  16/big-signed-integer).
-define(T_INT32,  32/big-signed-integer).
-define(T_INT64,  64/big-signed-integer).

-define(RT_VOID, 1:?T_INT32).
-define(RT_ROWS, 2:?T_INT32).
-define(RT_SETKEYSPACE, 3:?T_INT32).
-define(RT_PREPARED, 4:?T_INT32).
-define(RT_SCHEMACHANGE, 5:?T_INT32).

-define(ER_UNPREPARED ,16#2500).

-define(CL_ONE, 16#0001).
-define(CL_TWO, 16#0002).
-define(CL_THREE, 16#0003).
-define(CL_QUORUM, 16#0004).
-define(CL_ALL, 16#0005).
-define(CL_LOCAL_QUORUM, 16#0006).
-define(CL_EACH_QUORUM, 16#0007).
-define(CL_SERIAL, 16#0008).
-define(CL_LOCAL_SERIAL, 16#0009).
-define(CL_LOCAL_ONE, 16#000A).

%%==============================================================================
%% END OF FILE
