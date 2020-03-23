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
