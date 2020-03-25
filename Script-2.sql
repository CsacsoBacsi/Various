create type t_test AS (status text, curr text, tx_ts timestamp) ;
drop table test_arr ;
drop table test_arr_inp ;
create table test_arr (id integer, attrs t_test[]) ;
create table test_arr_inp (id integer, status text, curr text, tx_ts timestamp) ;
insert into test_arr (id, attrs)
values (1, ARRAY ['("O", "GBP", "2020-03-10 12:05:59")'::t_test,
                  '("S", "GBP", "2020-03-16 12:15:20")'::t_test]) ; 

select id, status, curr, tx_ts from test_arr, unnest (attrs) ; 

update test_arr
  set attrs = array_append (attributes, '("X", "EUR", "2020-03-10 12:56:48")'::t_test
  where id = 1 ; 

update test_arr
  set attrs = array_prepend ('("Y", "HUF", "2020-03-10 12:58:49")'::t_test 
  where inv_id = 1 ; 

update test_arr
  set attrs = attributes || ARRAY ['("C", "GBP", "2020-03-10 12:35:29")'::t_arr] 
  where _id = 1 ; 

update test_arr
  set attrs [1].status = 'O'
  where id = 1 ; 
 
delete from test_arr_inp ;

insert into test_arr_inp (id, status, curr, tx_ts)
select 2, 'Y'::text, 'GBP'::text, '2020-03-23 16:03:00'::timestamp ;

alter table test_arr add constraint pk_test_arr primary key (id) ;

insert into test_arr as t (id, attrs)
select id, array_agg (array [(status::text, curr::text, tx_ts::timestamp)::t_test]) from test_arr_inp as inp group by id
on conflict on constraint pk_test_arr
do update set attrs = array_cat (t.attrs, excluded.attrs) ;

select * from test_arr_inp ;
select id, k.* from test_arr, unnest (attrs) as k ;
select id, cardinality (attrs) from test_arr, unnest (attrs) as k ;


with src as (
  SELECT base_tb.relname as table_name, pt.relname as partition_name, 
         count (*) over (partition by base_tb.relname) as total_rn 
  FROM pg_class as base_tb JOIN pg_inherits i ON i.inhparent = base_tb.oid JOIN pg_class pt ON pt.oid = i.inhrelid 
)
SELECT table_name, partition_name, total_rn 
FROM src 
;

CREATE OR REPLACE PROCEDURE del_prt (IN p_parent_table_name text, IN p_del_parent boolean)
LANGUAGE plpgsql
AS $$
DECLARE
query text ;
this_partition text ;

csr CURSOR (parent_table_name text) FOR
    WITH src as (
         SELECT base_tb.relname as table_name, pt.relname as partition_name
         FROM pg_class as base_tb JOIN pg_inherits i ON i.inhparent = base_tb.oid JOIN pg_class pt ON pt.oid = i.inhrelid 
    )
    SELECT partition_name 
    FROM src
    WHERE table_name = parent_table_name ;

BEGIN
        
    OPEN csr (parent_table_name := p_parent_table_name) ;
    LOOP
        FETCH csr into this_partition ;
        EXIT WHEN NOT FOUND ;
       
        query := 'ALTER TABLE ' || quote_ident (p_parent_table_name) || ' DETACH PARTITION ' || quote_ident (this_partition) ;
        EXECUTE query ;
        RAISE NOTICE 'Partition % detached', this_partition ;
       
        EXECUTE 'DROP TABLE ' || quote_ident (this_partition) ;
        RAISE NOTICE 'Table % dropped', this_partition ;
         
    END LOOP ;
    CLOSE csr ;
   
    IF p_del_parent THEN
        EXECUTE 'DROP TABLE ' || quote_ident (p_parent_table_name) ;
        RAISE NOTICE 'Parent table % dropped', p_parent_Table_name ;
    END IF ;
   
    COMMIT ;  
END
$$;

CALL del_prt ('mytest'::text, true::boolean) ;

WITH src as (
         SELECT base_tb.relname as table_name, pt.relname as partition_name, 
                count (*) over (partition by base_tb.relname) as total_rn 
         FROM pg_class as base_tb JOIN pg_inherits i ON i.inhparent = base_tb.oid JOIN pg_class pt ON pt.oid = i.inhrelid 
    )
    SELECT table_name, partition_name, total_rn 
    FROM src
    ;