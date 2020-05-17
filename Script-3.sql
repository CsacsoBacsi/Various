drop table nullak ;
drop table nullak2 ;

create table nullak  (a integer not null, b integer not null, c integer, d text, e text not null) ;
create table nullak2  (a integer, b integer, c integer, d integer, e integer) ;

delete from nullak ;
delete from nullak2 ;

insert into nullak (a, b, c, d, e)
values (1, 2, 3, null, 'hi'), (5, 7, null, 'hi', 'hi'), (1, 6, 7, 'hi', 'hi'), (9, 10, null, null, 'hi') ;

insert into nullak2 (a, b, c, d, e)
values (1, 2, 3, null, null) ;

select a from nullak where (nullak is null) ;

select * from nullak where row (a,b,d) is not null ;

CREATE OR REPLACE PROCEDURE gener (IN p_name text)
LANGUAGE plpgsql
AS $proc$

DECLARE

l_proc text ;
l_col text ;
l_row text ;

csr CURSOR FOR
  SELECT column_name,
         data_type,
         column_default,
         is_nullable,
         character_maximum_length,
         numeric_precision
   FROM  information_schema.columns
   WHERE table_name = p_name
     AND is_nullable = 'YES'
   ORDER BY ordinal_position
;

BEGIN

    OPEN csr ;
    l_row := 'row (' ;
    LOOP
        FETCH csr INTO l_col ;
	    EXIT WHEN NOT FOUND ;
	    l_row := l_row || l_col || ', ' ; 
	END LOOP ;
    CLOSE csr ;
    l_row := substring (l_row, 1, length (l_row) - 2) ;
    l_row := l_row || ')' ;
    l_proc := 'create or replace function generated ()' || chr (10) ||
              'returns boolean' || chr (10) ||
              'as $p$' || chr (10) ||
              'declare' || chr (10) ||
              '    l_result integer ;' || chr (10) ||
              'begin' || chr (10) ||
              '    select 1 from nullak2 where ' || l_row || ' is not null into l_result ;' || chr (10) || 
              '    if l_result is null then' || chr (10) ||
              '        return false ;' || chr (10) ||
              '    else' || chr (10) ||
              '        return true ;' || chr (10) ||
              '    end if ;' || chr (10) ||
              'end ;' || chr (10) ||
              '$p$ ' || chr (10) ||
              'language plpgsql ;' || chr (10) ;
    EXECUTE l_proc ;
    RAISE NOTICE 'Proc created' ;
       
    COMMIT ;  
END ;
$proc$ ;

call gener ('nullak') ;
select generated () ;

SELECT column_name,
         data_type,
         column_default,
         is_nullable,
         character_maximum_length,
         numeric_precision
   FROM  information_schema.columns
   WHERE table_name = 'nullak' ;
