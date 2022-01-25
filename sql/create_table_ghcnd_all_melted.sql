drop table if exists ghcnd_all;
create table ghcnd_all (
    id text,
    obs_year smallint,
    obs_month smallint,
    element text,
    measure int,
    mflag text,
    qflag text,
    sflag text)