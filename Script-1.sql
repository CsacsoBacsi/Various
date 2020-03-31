create table mytest (id integer, tx date) partition by range (tx) ;

CREATE TABLE p1 PARTITION OF mytest
FOR VALUES FROM ('2020-01-01') TO ('2020-01-02') ;

CREATE TABLE p2 PARTITION OF mytest
FOR VALUES FROM ('2020-01-03') TO ('2020-01-04') ;

CREATE TABLE p3 PARTITION OF mytest
FOR VALUES FROM ('2020-01-05') TO ('2020-01-06') ;

insert into mytest (id, tx)
values (1, '2020-01-01'),  (2, '2020-01-03'),  (1, '2020-01-05'),  (3, '2020-01-05')   
;

select * from mytest ;

create table mytest2 (like mytest including all) partition by range (tx) ;

drop table mytest2 ;

alter table mytest detach partition p3 ;
alter table p3 add constraint p3_check1 check (tx >= '2020-01-05'::date AND tx < '2020-01-06'::date)

alter table mytest2 attach partition p3 FOR VALUES FROM ('2020-01-03') TO ('2020-01-04') ;

FOR VALUES FROM ('2020-01-03') TO ('2020-01-04') ; 
select * from p3 ;

with base as (
    select 1 as id, null as status, 'GBP' as curr, '2020-01-01'::date as dt union
    select 1 as id, 'O'  as status, null as curr,  '2020-01-02'::date as dt union
    select 1 as id, null as status, 'EUR' as curr, '2020-01-03'::date as dt union
    select 1 as id, null as status, 'GBP' as curr, '2020-01-04'::date as dt union
    select 1 as id, 'S'  as status, null as curr,  '2020-01-05'::date as dt union 
    select 1 as id, 'C'  as status, null as curr,  '2020-01-06'::date as dt union
    select 2 as id, null as status, 'GBP' as curr, '2020-01-01'::date as dt union
    select 2 as id, null as status, 'HUF' as curr, '2020-01-02'::date as dt union
    select 2 as id, 'O'  as status, null as curr,  '2020-01-03'::date as dt union
    select 2 as id, null as status, 'GBP' as curr, '2020-01-04'::date as dt union
    select 2 as id, 'S'  as status, null as curr,  '2020-01-05'::date as dt union 
    select 2 as id, null as status, 'EUR' as curr, '2020-01-06'::date as dt union
    select 3 as id, 'O'  as status, null as curr,  '2020-01-01'::date as dt union
    select 3 as id, null as status, 'GBP' as curr, '2020-01-02'::date as dt union
    select 3 as id, null as status, 'EUR' as curr, '2020-01-03'::date as dt union
    select 3 as id, null as status, 'GBP' as curr, '2020-01-04'::date as dt union
    select 3 as id, 'S'  as status, null as curr,  '2020-01-05'::date as dt union 
    select 3 as id, 'C'  as status, null as curr,  '2020-01-06'::date as dt union
    select 4 as id, 'O'  as status, null as curr,  '2020-01-01'::date as dt union
    select 4 as id, 'C'  as status, null as curr,  '2020-01-02'::date as dt union
    select 4 as id, null as status, 'GBP' as curr, '2020-01-03'::date as dt union
    select 4 as id, null as status, 'EUR' as curr, '2020-01-04'::date as dt union
    select 4 as id, 'X'  as status, null as curr,  '2020-01-05'::date as dt union 
    select 4 as id, null as status, 'GBP' as curr, '2020-01-06'::date as dt union
    select 5 as id, 'O'  as status, null as curr,  '2020-01-01'::date as dt union
    select 5 as id, null as status, 'HUF' as curr, '2020-01-02'::date as dt union
    select 5 as id, null as status, 'GBP' as curr, '2020-01-03'::date as dt union
    select 5 as id, null as status, 'HUF' as curr, '2020-01-04'::date as dt union
    select 5 as id, null as status, 'GBP' as curr, '2020-01-05'::date as dt union 
    select 5 as id, 'C'  as status, null as curr,  '2020-01-06'::date as dt
),
prt as (
    select id, status, curr, dt,
           coalesce (sum (case when status is not null then 1 end) over (partition by id order by dt), 1) as grp_status,
           coalesce (sum (case when curr is not null then 1 end) over (partition by id order by dt), 1) as grp_curr
    from   base
)
select id, status, curr, grp_status, grp_curr, dt,
       max (status) over (partition by id, grp_status) as max_status,
       max (curr) over (partition by id, grp_curr) as max_curr
from prt
order by id, dt
;

with base as (
    select 1 as id, 'O' as status, '2020-01-01'::date as dt union
    select 1 as id, null as status, '2020-01-02'::date as dt union
    select 1 as id, null as status, '2020-01-03'::date as dt union
    select 1 as id, null as status, '2020-01-04'::date as dt union 
    select 1 as id, 'C' as status, '2020-01-05'::date as dt union 
    select 1 as id, null as status, '2020-01-06'::date as dt
)
select id, status,
       first_value (status) over (partition by id order by dt) as first_status,
       last_value (status) over (partition by id order by dt) as last_status
from   base
;

create table t1 (t1_id int4, t1_desc text, t1_attr1 text, t1_attr2 text)
create table t2 (t2_id int4, t1_id int4, t2_desc text, t2_attr1 text, t2_attr2 text)
create table t3 (t3_id int4, t2_id int4, t3_desc text, t3_attr1 text, t3_attr2 text)

create table t1 (t1_id int4, t1_desc text, t1_attr1 text, t1_attr2 text)
create table t_bridge (t1_id int4, t2_id int4, creation_dt date)
create table t2 (t2_id int4, t2_desc text, t2_attr1 text, t2_attr2 text)

-- Rules?
-- Can we insert into bridge without parent tables?

create type mytype as (status text, curr text, dt date) ;
create table myarr (id int4, attr mytype []) ;

delete from myarr ;
insert into myarr (id, attr)
values (1, array [row ('O'::text, 'EUR'::text, '2020-01-01'::date)::mytype,
                  row (null::text, 'HUF'::text, '2020-01-04'::date)::mytype,
                  row ('C'::text, null::text, '2020-01-03'::date)::mytype,
                  row (null::text, 'GBP'::text, '2020-01-02'::date)::mytype])
;

select id, status, curr, dt
    from myarr, unnest (attr)
;

with src as (
    select id, status, curr, dt
    from myarr, unnest (attr)
),
arr as (
    select id, array [row (status, curr, dt)::mytype] as attr, dt
    from src
),
agg as (
    select id,
           array_agg (attr order by dt) as attr 
    from   arr
    group by id
)
select id, status, curr, dt 
from   agg, unnest (attr)
;

with src as (
    select 1 as id, 'A' as class, '2020-01-07' as dt, 10 as val union
    select 2 as id, 'A' as class, '2020-01-01' as dt, 30 as val union
    select 3 as id, 'B' as class, '2020-01-03' as dt, 60 as val union
    select 4 as id, 'A' as class, '2020-01-05' as dt, 90 as val union
    select 5 as id, 'B' as class, '2020-01-10' as dt, 20 as val union
    select 6 as id, 'A' as class, '2020-01-06' as dt, 5 as val union
    select 7 as id, 'A' as class, '2020-01-09' as dt, 15 as val union
    select 8 as id, 'B' as class, '2020-01-04' as dt, 70 as val
)
select id, class, dt, val,
       max (val) over () as max_val,
       max (val) over (order by dt) as running_max_val,
       max (val) over (order by dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as max_val2,
       max (val) over (order by dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_max_val2,
       max (val) over (partition by class) as class_max_val,
       max (val) over (partition by class order by dt) as running_class_max_val
from   src
order by class, dt
;

-- ****************** Stress test *******************

create or replace view v_gen as 
with statuses as (
    select 'O' as status, null as curr, '2020-01-01 15:47:30'::timestamp as start_date union
    select null as status, 'GBP' as curr, '2020-01-01 15:46:30'::timestamp as start_date union
    select 'C' as status, null as curr, '2020-01-01 15:42:30'::timestamp as start_date
),
gen as (
    select generate_series (1, 1000000, 1) as id
)
select gen.id, s.status, s.curr, s.start_date
from   gen
cross join statuses s
;

create type stype as (status text, curr text, start_date timestamp) ;
create table stest (id int4, attrs stype []) ;

insert into stest as t (id, attrs)
select id, array_agg (array [(status::text, curr::text, start_date::timestamp)::stype]) from v_gen group by id ;

delete from stest ;

select count (*) from stest, unnest (attrs) ;

with base as (
    select id, status, curr, start_date from stest, unnest (attrs)
),
prt as (
    select id, status, curr, start_date,
           coalesce (sum (case when status is not null then 1 end) over (partition by id order by start_date), 1) as grp_status,
           coalesce (sum (case when curr is not null then 1 end) over (partition by id order by start_date), 1) as grp_curr,
           lead (start_date) over (partition by id order by start_date) - interval '1 millisecond' as end_date
    from   base
)
select id, status, curr, grp_status, grp_curr, start_date, end_date,
       max (status) over (partition by id, grp_status) as max_status,
       max (curr) over (partition by id, grp_curr) as max_curr
from prt
order by id, start_date
;

with cubcus as (
    select 1 as id, 'A' as name, 100 as SIC, '2020-01-01 12:05:10'::timestamp as sd union
    select 1 as id, 'B' as name, 100 as SIC, '2020-01-01 12:06:10'::timestamp as sd union
    select 1 as id, 'C' as name, 100 as SIC, '2020-01-01 12:06:30'::timestamp as sd union
    select 1 as id, 'D' as name, 100 as SIC, '2020-01-01 12:07:10'::timestamp as sd
),
cuatrb as (
    select 1 as id, 10 as rating, '2020-01-01 12:05:11'::timestamp as sd union
    select 1 as id, 12 as rating, '2020-01-01 12:07:11'::timestamp as sd
),
joined as (
    select coalesce (c.id, b.id) as id,
           c.name, c.sic,
           coalesce (c.sd, b.sd) as sd,
           b.rating
    from   cubcus as c
    full outer join
           cuatrb b
      on (c.id = b.id and c.sd = b.sd)
) select * from joined ;

select id, array_agg (array [(name, sic, sd, rating)]) as attrs
from   joined
group by id
;