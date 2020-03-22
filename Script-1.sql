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

alter table mytest2 attach partition p3 ;

FOR VALUES FROM ('2020-01-03') TO ('2020-01-04') ; 


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
       max (status) over (partition by id, grp_status order by dt asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as max_status,
       max (curr) over (partition by id, grp_curr order by dt asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as max_curr
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