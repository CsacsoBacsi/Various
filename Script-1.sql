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

delete from "My".stest ;

select count (*) from "My".stest, unnest (attrs) ;

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
order by id desc, start_date
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

Assumption: for each id there is a single I row from each topic
Scenario 1: pure inserts
1. I row
   Exists
   No
      Insert id and first array row
   Yes
      array_append row
2. U row
   Exists
   Yes
      array_append row
   No
      Insert id and first array row

Scenario 2: first array row is only manipulated by inserts, updates are all appended as they come in
1. I row
   Exists
   No
      Insert id and first array row
   Yes
      First array row result of insert
      Yes
         Update first array row
      No
         array_prepend row
2. U row
   Exists
   Yes
      array_append row
   No
      Insert id and first array row

Scenario 3: when updating array (array_append), always reorder array rows
1. I row
   Exists 
   No
      Insert id and first array row
   Yes
      First array row result of insert
      Yes
         Update first array row
      No
         array_prepend row
2. U row
   Exists
   Yes
      array_append row
      Optional: order array, update whole array
   No
      Insert id and first array row

Scenario 4: when updating array, check values if changed or not -- Convoluted. Collapsing and splitting rows dynamically
1. I row
   Exists
   No
      Insert id and first array row
   Yes
      First array row result of insert
      Yes
         Update first array row
      No
         array_prepend row
2. U row
   Exists
   Yes
      array_append row
      Optional: order array, update whole array
         Optional: check last array row if no change ignore row
   No
      Insert id and first array row

Scenario 5: optionally hold (and continously update) current values in separate table (SCD2 + SCD3)
1. I row
   Exists
   No
      Insert id and row
   Yes
      Row result of insert
      Yes
         Update row
      No
         Keep row as is
2. U row
   Exists
   Yes
      Update row
   No
      Insert id and row
      
-- SELECT FOR UPDATE test
create table lockt (id int4, mydesc text) ;

insert into lockt (id, mydesc)
  values (1, 'First'), (2, 'second'), (3, 'third'), (4, 'fourth'), (5, 'fifth') ;
 
select * from lockt where id = 3 for update ;
select * from "My".lockt where id = 3 ;

CREATE OR REPLACE PROCEDURE lo ()
LANGUAGE plpgsql
AS $$
DECLARE


declare 
  cnt integer ;
BEGIN

	SELECT 1 INTO cnt FROM lockt WHERE id = 6 FOR UPDATE ;
    IF cnt = 1 THEN
        RAISE NOTICE 'Lock succeeded' ;
    ELSE
        RAISE NOTICE 'Lock failed' ;
    END IF ;
    PERFORM pg_sleep (20) ; -- Sleep for 20 secs locking the row
END
$$
;

call lo () ;


drop table mtable ;
drop type sometype cascade ;
drop type t_mtable cascade ;
create type sometype as (id integer, txtype text, status text, curr text, rating integer, tx_ts timestamp) ;
create type t_mtable as (txtype text, status text, curr text, rating integer, tx_ts timestamp) ;
create table mtable (id integer, attrs t_mtable []) ;

CREATE OR REPLACE PROCEDURE ins (myrow sometype, scen integer)
LANGUAGE plpgsql
AS $$
DECLARE

l_txtype text ;

BEGIN

IF scen = 1 THEN -- Scenario 1: pure inserts
    
    IF myrow.txtype = 'I' THEN -- 'I'nsert row
	    l_txtype := NULL ;
	    SELECT attrs[1].txtype INTO l_txtype FROM mtable WHERE id = myrow.id FOR UPDATE ;
	    IF l_txtype IS NOT NULL THEN -- ID exists
	        UPDATE mtable t -- Append row to array
	           SET attrs = array_append (attrs, (myrow.txtype, myrow.status::text, myrow.curr::text, myrow.rating::integer, myrow.tx_ts::timestamp)::t_mtable)
	        WHERE id = myrow.id ; 
	    ELSE -- ID does not exist yet
	        INSERT INTO mtable (id, attrs) -- Insert id and first array row
	        VALUES (myrow.id, ARRAY [(myrow.txtype, myrow.status::text, myrow.curr::text, myrow.rating::integer, myrow.tx_ts::timestamp)::t_mtable]) ; 
	    END IF ;
	   
    ELSE -- 'U'pdate row
    
        l_txtype := NULL ;
	    SELECT attrs[1].txtype INTO l_txtype FROM mtable WHERE id = myrow.id FOR UPDATE ;
	    IF l_txtype IS NOT NULL THEN -- ID exists
            UPDATE mtable t -- Append row to array
	           SET attrs = array_append (attrs, (myrow.txtype, myrow.status::text, myrow.curr::text, myrow.rating::integer, myrow.tx_ts::timestamp)::t_mtable) -- array_append row
	        WHERE id = myrow.id ;
        ELSE -- ID does not exist
            INSERT INTO mtable (id, attrs) -- Insert id and first array row
	        VALUES (myrow.id, ARRAY [(myrow.txtype::text, myrow.status::text, myrow.curr::text, myrow.rating::integer, myrow.tx_ts::timestamp)::t_mtable]) ; 
	    END IF ;
	END IF ;

ELSIF scen = 2 THEN -- First array row is only manipulated by inserts, updates are all appended as they come in

	IF myrow.txtype = 'I' THEN -- 'I'nsert row
	    l_txtype := NULL ;
	    SELECT attrs[1].txtype INTO l_txtype FROM mtable WHERE id = myrow.id FOR UPDATE ;  
	    IF l_txtype = 'I' THEN -- ID exists and first array row is of 'I'nsert type
	        UPDATE mtable t -- Update first row of array
	           SET attrs[1].txtype = myrow.txtype,
	               attrs[1].status = myrow.status,
	               attrs[1].curr   = myrow.curr,
	               attrs[1].rating = myrow.rating
	               -- attrs[1].tx_ts  = myrow.tx_ts
	        WHERE id = myrow.id ;
	    ELSIF l_txtype = 'U' THEN -- ID exists and first array row is of 'U'pdate type
	        UPDATE mtable t -- Insert 'I' row in first place (before 'U' row) in the array
	           SET attrs = array_prepend ((myrow.txtype, myrow.status::text, myrow.curr::text, myrow.rating::integer, myrow.tx_ts::timestamp)::t_mtable, attrs)
	        WHERE id = myrow.id ;
	    ELSE -- ID does not exist yet
	        INSERT INTO mtable (id, attrs) -- Insert id and first array row
	        VALUES (myrow.id, ARRAY [(myrow.txtype, myrow.status::text, myrow.curr::text, myrow.rating::integer, myrow.tx_ts::timestamp)::t_mtable]) ; 
	    END IF ;
	   
	ELSE -- 'U'pdate row
	
	    l_txtype := NULL ;
	    SELECT attrs[1].txtype INTO l_txtype FROM mtable WHERE id = myrow.id FOR UPDATE ;
	    IF l_txtype IS NOT NULL THEN -- ID exists
            UPDATE mtable t
	           SET attrs = array_append (attrs, (myrow.txtype, myrow.status::text, myrow.curr::text, myrow.rating::integer, myrow.tx_ts::timestamp)::t_mtable) -- array_append row
	        WHERE id = myrow.id ;
        ELSE -- ID does not exist
            INSERT INTO mtable (id, attrs) -- Insert id and first array row
	        VALUES (myrow.id, ARRAY [(myrow.txtype::text, myrow.status::text, myrow.curr::text, myrow.rating::integer, myrow.tx_ts::timestamp)::t_mtable]) ; 
	    END IF ;
	END IF ;

ELSIF scen = 3 THEN -- When updating array (array_append), always reorder array rows

	IF myrow.txtype = 'I' THEN -- 'I'nsert row
	    l_txtype := NULL ;
	    SELECT attrs[1].txtype INTO l_txtype FROM mtable WHERE id = myrow.id FOR UPDATE ;  
	    IF l_txtype = 'I' THEN -- ID exists and first array row is of 'I'nsert type
	        UPDATE mtable t -- Update first row of array
	           SET attrs[1].txtype = myrow.txtype,
	               attrs[1].status = myrow.status,
	               attrs[1].curr   = myrow.curr,
	               attrs[1].rating = myrow.rating
	               -- attrs[1].tx_ts  = myrow.tx_ts
	        WHERE id = myrow.id ;
	        RAISE NOTICE 'Scenario 3, Insert, First row is I, first array row update' ;
	    ELSIF l_txtype = 'U' THEN -- ID exists and first array row is of 'U'pdate type
	        UPDATE mtable t -- Insert 'I' row in first place (before 'U' row) in the array
	           SET attrs = array_prepend ((myrow.txtype, myrow.status::text, myrow.curr::text, myrow.rating::integer, myrow.tx_ts::timestamp)::t_mtable, attrs)
	        WHERE id = myrow.id ;
	        RAISE NOTICE 'Scenario 3, Insert, First row is U, append row to array' ;
	    ELSE -- ID does not exist yet
	        INSERT INTO mtable (id, attrs) -- Insert id and first array row
	        VALUES (myrow.id, ARRAY [(myrow.txtype::text, myrow.status::text, myrow.curr::text, myrow.rating::integer, myrow.tx_ts::timestamp)::t_mtable]) ;
	        RAISE NOTICE 'Scenario 3, Insert, First row does not exist, insert first row' ;
	    END IF ;
	   
	ELSE -- 'U'pdate row
	
	    l_txtype := NULL ;
	    SELECT attrs[1].txtype INTO l_txtype FROM mtable WHERE id = myrow.id FOR UPDATE ;
	    RAISE NOTICE 'Tx type: %', l_txtype ;
	    RAISE NOTICE 'Myrowid: %', myrow.id ;
	    RAISE NOTICE 'Myrowid: %', myrow.tx_ts ;
	    IF l_txtype IS NOT NULL THEN -- ID exists
            UPDATE mtable as t
	           SET attrs = attrs || (myrow.txtype, myrow.status::text, myrow.curr::text, myrow.rating::integer, myrow.tx_ts::timestamp)::t_mtable -- array_append row
	        WHERE id = myrow.id ;
	        RAISE NOTICE 'Scenario 3, Update, First row exists, append row to array' ;

	        WITH src AS ( -- After appending to the array, re-order array rows in the correct timestamp order (only for 'U'pdate rows)
                SELECT id, txtype, status, curr, rating, tx_ts
                FROM   mtable, unnest (attrs)
                WHERE  id = 1
                ORDER BY tx_ts
            ),
            ordered AS (
                SELECT id, array_agg (row (txtype::text, status::text, curr::text, rating::integer, tx_ts::timestamp)::t_mtable) as new_attrs 
                FROM   src
                GROUP BY id
            ) -- select txtype, status, curr, rating, tx_ts, array_dims (new_attrs), new_attrs [0:2][1:1] from ordered, unnest (new_attrs) ;         
            UPDATE mtable as t
               SET attrs = o.new_attrs
            FROM   ordered as o
            WHERE  t.id = 1
              AND  t.id = o.id ;
	        
            RAISE NOTICE 'Scenario 3, Update, First row exists, re-order array' ;
	       
        ELSE -- ID does not exist
            INSERT INTO mtable (id, attrs) -- Insert id and first array row
	        VALUES (myrow.id, ARRAY [(myrow.txtype::text, myrow.status::text, myrow.curr::text, myrow.rating::integer, myrow.tx_ts::timestamp)::t_mtable]) ;
	        RAISE NOTICE 'Scenario 3, Update, First row does not exist, insert first row' ;
	    END IF ;
	END IF ;

END IF ;

END
$$
;

delete from mtable ;
call ins ((1, 'I', 'A', 'GBP', 2, '2020-04-04 10:30:10'::timestamp)::sometype, 3) ;
call ins ((1, 'I', 'V', 'GBP', 2, '2020-04-04 10:35:10'::timestamp)::sometype, 3) ;

call ins ((1, 'U', 'A', 'GBP', 2, now()::timestamp)::sometype, 3) ;
call ins ((1, 'U', 'X', 'GBP', 2, '2020-04-04 10:40:10'::timestamp)::sometype, 3) ;
call ins ((1, 'U', 'Y', 'GBP', 2, '2020-04-04 10:32:10'::timestamp)::sometype, 3) ;

select id, txtype, status, curr, rating, tx_ts, array_length (attrs, 1) as arr_len from mtable, unnest (attrs) ;
select count (*) FROM mtable ;
select id, attrs[1].txtype, attrs[1].status from mtable where id = 1 ;

WITH src AS ( -- After appending to the array, re-order array rows in the correct timestamp order (only for 'U'pdate rows)
    SELECT id, txtype, status, curr, rating, tx_ts
    FROM   mtable, unnest (attrs)
    WHERE  id = 1
    ORDER BY tx_ts
),
ordered AS (
    SELECT id, array_agg (row (txtype::text, status::text, curr::text, rating::integer, tx_ts::timestamp)::t_mtable) as new_attrs 
    FROM   src
    GROUP BY id
) -- select txtype, status, curr, rating, tx_ts, array_dims (new_attrs), new_attrs [0:2][1:1] from ordered, unnest (new_attrs) ;         
UPDATE mtable as t
   SET attrs = o.new_attrs
  FROM   ordered as o
 WHERE  t.id = 1
   AND  t.id = o.id ;            
             
select array_dims (attrs) from mtable ;
update mtable set attrs = array_append (attrs, row ('X'::text, 'Y'::text, 'HUF'::text, 3::integer, '2002-01-01 10:10:10'::timestamp)::t_mtable) ;
            
select txtype, status, curr, rating, tx_ts from mtable, unnest (attrs) ;
select attrs || row ('X'::text, 'Y'::text, 'HUF'::text, 3::integer, '2002-01-01 10:10:10'::timestamp)::t_mtable from mtable, unnest (attrs) ;
            
select attrs[2:3] from mtable ;

WITH src AS ( -- After appending to the array, re-order array rows in the correct timestamp order (only for 'U'pdate rows)
    SELECT id, txtype, status, curr, rating, tx_ts
    FROM   mtable, unnest (attrs)
    WHERE  id = 1
),
ordered AS (
    -- SELECT id, array_agg (row (txtype::text, status::text, curr::text, rating::integer, tx_ts::timestamp)::t_mtable) as new_attrs 
    SELECT id, array_agg (array [(txtype::text, status::text, curr::text, rating::integer, tx_ts::timestamp)::t_mtable] order by tx_ts) as new_attrs,
    array_agg (row(txtype::text, status::text, curr::text, rating::integer, tx_ts::timestamp)::t_mtable order by tx_ts) as new_attrs2
    FROM   src
    GROUP BY id
) -- select id, txtype, status, curr, rating, tx_ts, array_dims (new_attrs), new_attrs[3:3][1:1], array_dims (new_attrs2) from ordered, unnest (new_attrs, new_attrs2) ;
select * from ordered, unnest (new_attrs, new_attrs2) ;