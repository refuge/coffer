%
% coffer.hrl
%

-type blob_id() :: binary().
-type data() :: binary().

-record(ref, {backend, sref, active=true}).
