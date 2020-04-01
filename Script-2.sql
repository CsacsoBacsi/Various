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

drop function dt ;
IN p_schema text DEFAULT 'public', IN p_type text DEFAULT 't'

CREATE OR REPLACE PROCEDURE dt (IN p_schema text DEFAULT 'public', IN p_type text DEFAULT '*')
RETURNS TABLE ("Shema" text, "Name" text, "Type" text, "Owner" text) AS
$func$
BEGIN

IF p_type = '*' THEN
	RETURN QUERY
	SELECT n.nspname::text as "Schema",
	       c.relname::text as "Name",
	       CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' END::text as "Type",
	       pg_catalog.pg_get_userbyid (c.relowner)::text as "Owner"
	FROM   pg_catalog.pg_class c
	LEFT JOIN pg_catalog.pg_namespace n
	   ON  n.oid = c.relnamespace
	WHERE  c.relkind IN ('r','v','p', 'm')
	       AND n.nspname <> 'pg_catalog'
	       AND n.nspname <> 'information_schema'
	       AND n.nspname !~ '^pg_toast'
	       AND pg_catalog.pg_table_is_visible (c.oid)
	       AND n.nspname = p_schema
	ORDER BY n.nspname, c.relname, CASE c.relkind WHEN 'r' THEN 1 WHEN 'p' THEN 2 WHEN 'v' THEN 3 WHEN 'm' THEN 4 END ;
ELSIF p_type = 'tp' THEN
	RETURN QUERY
	SELECT n.nspname::text as "Schema",
	       c.relname::text as "Name",
	       CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' END::text as "Type",
	       pg_catalog.pg_get_userbyid (c.relowner)::text as "Owner"
	FROM   pg_catalog.pg_class c
	LEFT JOIN pg_catalog.pg_namespace n
	   ON  n.oid = c.relnamespace
	WHERE  c.relkind IN ('r', 'p')
	       AND n.nspname <> 'pg_catalog'
	       AND n.nspname <> 'information_schema'
	       AND n.nspname !~ '^pg_toast'
	       AND pg_catalog.pg_table_is_visible (c.oid)
	       AND n.nspname = p_schema
	ORDER BY n.nspname, c.relname, CASE c.relkind WHEN 'r' THEN 1 WHEN 'p' THEN 2 WHEN 'v' THEN 3 WHEN 'm' THEN 4 END ;
ELSE
	RETURN QUERY
	SELECT n.nspname::text as "Schema",
	       c.relname::text as "Name",
	       CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' END::text as "Type",
	       pg_catalog.pg_get_userbyid (c.relowner)::text as "Owner"
	FROM   pg_catalog.pg_class c
	LEFT JOIN pg_catalog.pg_namespace n
	   ON  n.oid = c.relnamespace
	WHERE  c.relkind IN (p_type)
	       AND n.nspname <> 'pg_catalog'
	       AND n.nspname <> 'information_schema'
	       AND n.nspname !~ '^pg_toast'
	       AND pg_catalog.pg_table_is_visible (c.oid)
	       AND n.nspname = p_schema
	ORDER BY n.nspname, c.relname, CASE c.relkind WHEN 'r' THEN 1 WHEN 'p' THEN 2 WHEN 'v' THEN 3 WHEN 'm' THEN 4 END ;
END IF ;

END
$func$
LANGUAGE 'plpgsql'
;

select * from dt ('My', '') ;

SELECT n.nspname::text as "Schema",
       c.relname::text as "Name",
       CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table ' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' END::text as "Type",
       pg_catalog.pg_get_userbyid (c.relowner)::text as "Owner"
FROM   pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n
   ON  n.oid = c.relnamespace
WHERE  c.relkind IN ('r','v','p', 'm')
       AND n.nspname <> 'pg_catalog'
       AND n.nspname <> 'information_schema'
       AND n.nspname !~ '^pg_toast'
       AND pg_catalog.pg_table_is_visible (c.oid)
ORDER BY n.nspname, c.relname, CASE c.relkind WHEN 'r' THEN 1 WHEN 'p' THEN 2 WHEN 'v' THEN 3 WHEN 'm' THEN 4 END ;

CREATE OR REPLACE PROCEDURE dt2 (IN p_schema text default 'public', IN p_type text default 'tp')
LANGUAGE plpgsql
AS $$
DECLARE
query text ;
l_schema text ;
l_name text ;
l_type text ;
l_owner text ;

csr CURSOR (schema_name text) FOR
    SELECT n.nspname::text as "Schema",
	       c.relname::text as "Name",
	       CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table ' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' END::text as "Type",
	       pg_catalog.pg_get_userbyid (c.relowner)::text as "Owner"
	FROM   pg_catalog.pg_class c
	LEFT JOIN pg_catalog.pg_namespace n
	   ON  n.oid = c.relnamespace
	WHERE  c.relkind IN ('r','v','p', 'm')
	       AND n.nspname <> 'pg_catalog'
	       AND n.nspname <> 'information_schema'
	       AND n.nspname !~ '^pg_toast'
	       AND pg_catalog.pg_table_is_visible (c.oid)
	       AND n.nspname = schema_name
	ORDER BY n.nspname, c.relname, CASE c.relkind WHEN 'r' THEN 1 WHEN 'p' THEN 2 WHEN 'v' THEN 3 WHEN 'm' THEN 4 END ;

csr_tp CURSOR (schema_name text) FOR
    SELECT n.nspname::text as "Schema",
	       c.relname::text as "Name",
	       CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table ' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' END::text as "Type",
	       pg_catalog.pg_get_userbyid (c.relowner)::text as "Owner"
	FROM   pg_catalog.pg_class c
	LEFT JOIN pg_catalog.pg_namespace n
	   ON  n.oid = c.relnamespace
	WHERE  c.relkind IN ('r','p')
	       AND n.nspname <> 'pg_catalog'
	       AND n.nspname <> 'information_schema'
	       AND n.nspname !~ '^pg_toast'
	       AND pg_catalog.pg_table_is_visible (c.oid)
	       AND n.nspname = schema_name
	ORDER BY n.nspname, c.relname, CASE c.relkind WHEN 'r' THEN 1 WHEN 'p' THEN 2 WHEN 'v' THEN 3 WHEN 'm' THEN 4 END ;

BEGIN
    
	RAISE NOTICE '% % % %', rpad ('Schema', 15, ' '), rpad ('Name', 30, ' '), rpad ('Type', 17, ' '), 'Owner'  ;
    RAISE NOTICE '% % % %', rpad ('-', 15, '-'), rpad ('-', 30, '-'), rpad ('-', 17, '-'), rpad ('-', 30, '-')  ;
   
	IF p_type = '*' THEN
	    OPEN csr (schema_name := p_schema) ;
	    
	    LOOP
	        FETCH csr into l_schema, l_name, l_type, l_owner ;
	        EXIT WHEN NOT FOUND ;
	       
	        RAISE NOTICE '% % % %', rpad (l_schema, 15, ' '), rpad (l_name, 30, ' '), rpad (l_type, 17, ' '), l_owner  ;
	         
	    END LOOP ;
	ELSIF p_type = 'tp' THEN
	    OPEN csr_tp (schema_name := p_schema) ;
	    
	    LOOP
	        FETCH csr_tp into l_schema, l_name, l_type, l_owner ;
	        EXIT WHEN NOT FOUND ;
	       
	        RAISE NOTICE '% % % %', rpad (l_schema, 15, ' '), rpad (l_name, 30, ' '), rpad (l_type, 17, ' '), l_owner  ;
	         
	    END LOOP ;
	ELSE
		OPEN csr (schema_name := p_schema) ;
	    
	    LOOP
	        FETCH csr into l_schema, l_name, l_type, l_owner ;
	        EXIT WHEN NOT FOUND ;
	       
	        RAISE NOTICE '% % % %', rpad (l_schema, 15, ' '), rpad (l_name, 30, ' '), rpad (l_type, 17, ' '), l_owner  ;
	         
	    END LOOP ;
	END IF ;
    CLOSE csr ;
   
END
$$;

call dt2 ('My') ;