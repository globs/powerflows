drop table public.powerflows_traces;
create table public.powerflows_traces
(
id              SERIAL PRIMARY KEY,
ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
function_name              text,
call_result          text
);


drop table public.powerflows_jobstorage;
create table public.powerflows_jobstorage
(
id              SERIAL PRIMARY KEY,
ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
function_name              text,
call_result          text, 
asset_id
);


drop table public.powerflows_assets;
create table public.powerflows_assets
(
id              SERIAL PRIMARY KEY,
ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
asset_name              text,
asset_type              text,
asset_address           text,
asset_json_def           text
);
