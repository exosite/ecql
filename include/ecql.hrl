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

%%==============================================================================
%% END OF FILE
