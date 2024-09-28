%%%   Licensed under the Apache License, Version 2.0 (the "License");
%%%   you may not use this file except in compliance with the License.
%%%   You may obtain a copy of the License at
%%%
%%%       http://www.apache.org/licenses/LICENSE-2.0
%%%
%%%   Unless required by applicable law or agreed to in writing, software
%%%   distributed under the License is distributed on an "AS IS" BASIS,
%%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%   See the License for the specific language governing permissions and
%%%   limitations under the License.
%%%

-ifndef(BRODCLI_HRL).
-define(BRODCLI_HRL, true).

-include_lib("kafka_protocol/include/kpro.hrl").
-include_lib("brod/include/brod.hrl").

-define(undef, undefined).

%% Is kafka error code
-define(IS_ERROR(EC), ((EC) =/= ?no_error)).

-define(KV(Key, Value), {Key, Value}).
-define(TKV(Ts, Key, Value), {Ts, Key, Value}).

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
