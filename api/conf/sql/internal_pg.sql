drop table public.powerflows_traces;
create table public.powerflows_traces
(
id              SERIAL PRIMARY KEY,
ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
function_name              text,
call_result          text
);
