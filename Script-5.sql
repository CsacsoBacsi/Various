
alter system set wal_level = logical ;
alter system set max_replication_slots = 2 ;

SELECT * FROM pg_create_logical_replication_slot('slot', 'test_decoding');
SELECT * FROM pg_create_logical_replication_slot('slot2', 'wal2json');
select * FROM pg_replication_slots ; 

create table repl (id integer, descr varchar (50)) ;
drop table repl ;

SELECT * FROM pg_logical_slot_get_changes('slot', NULL, NULL);
SELECT * FROM pg_logical_slot_get_changes('slot', NULL, NULL, 'pretty-print', '1');

insert into repl (id, descr) values (111, 'desc1') ;
update repl set descr = 'newdesc' where id = 111 ;
insert into repl (id, descr) values (222, 'desc2') ;

insert into repl (id, descr) values (333, 'desc3') ;
insert into repl (id, descr) values (444, 'desc4') ;

SELECT * FROM pg_logical_slot_peek_changes('slot', NULL, NULL, 'include-timestamp', 'on') where length (data) > 10;

SELECT pg_drop_replication_slot('slot');

