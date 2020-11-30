set schema 'udm' ;

begin ;

drop SEQUENCE if exists __tcache___id_seq cascade ;
drop table if exists __tcache__ cascade ;
drop SEQUENCE if exists __tresults___numb_seq cascade ;

create table if not exists udm.cstbl (id integer, name varchar (20), descr varchar (20), primary key (id)) ;
select tap.plan (8) ;
select tap.is (5, 4, 'five is five?') ;

-- cmp_ok
SELECT tap.cmp_ok ('this'::text, '='::text, 'that'::text, 'Does this equal that?'::text ) ;

-- has_pk
SELECT has_pk ('udm', 'cstbl', 'Has primary key?') ;
-- has_column
SELECT has_column ('udm', 'cstbl', 'name', 'Has name column?') ;
-- col_not_null
SELECT col_not_null ('udm', 'cstbl', 'name', 'Is name column not null?') ;
-- has_fk
SELECT has_fk ('udm', 'cstbl', 'Has a foreign key?') ;
-- col_is_fk
SELECT col_is_fk ('udm', 'cstbl', array ['name', 'descr'], 'Are columns foreign keys?') ;
-- col_is_unique
SELECT col_is_unique ('udm', 'cstbl', 'id', 'Is column unique?') ; -- Tests the unique constraint
select finish () ;

rollback ;

drop SEQUENCE if exists __tcache___id_seq cascade ;
drop table if exists __tcache__ cascade ;
drop SEQUENCE if exists __tresults___numb_seq cascade ;

CREATE OR REPLACE FUNCTION tap.plan(integer)
 RETURNS text
 LANGUAGE plpgsql
 STRICT
AS $function$
DECLARE
    rcount INTEGER;
BEGIN
	set schema 'tap' ;
    BEGIN
        EXECUTE '
            CREATE TEMP SEQUENCE __tcache___id_seq;
            CREATE TEMP TABLE __tcache__ (
                id    INTEGER NOT NULL DEFAULT nextval(''__tcache___id_seq''),
                label TEXT    NOT NULL,
                value INTEGER NOT NULL,
                note  TEXT    NOT NULL DEFAULT ''''
            );
            CREATE UNIQUE INDEX __tcache___key ON __tcache__(id);
            GRANT ALL ON TABLE __tcache__ TO PUBLIC;
            GRANT ALL ON TABLE __tcache___id_seq TO PUBLIC;

            CREATE TEMP SEQUENCE __tresults___numb_seq;
            GRANT ALL ON TABLE __tresults___numb_seq TO PUBLIC;
        ';

    EXCEPTION WHEN duplicate_table THEN
        -- Raise an exception if there's already a plan.
        EXECUTE 'SELECT TRUE FROM __tcache__ WHERE label = ''plan''';
      GET DIAGNOSTICS rcount = ROW_COUNT;
        IF rcount > 0 THEN
           RAISE EXCEPTION 'You tried to plan twice!';
        END IF;
    END;

    -- Save the plan and return.
    PERFORM tap._set('plan'::text, $1::integer );
    PERFORM tap._set('failed'::text, 0::integer );
    RETURN '1..'::text || $1::integer;
END;
$function$
;

CREATE OR REPLACE FUNCTION tap._set(text, integer)
 RETURNS integer
 LANGUAGE sql
AS $function$
    SELECT tap._set ($1::text, $2::int, ''::text)
$function$
;

CREATE OR REPLACE FUNCTION tap._set(text, integer, text)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
    rcount integer;
BEGIN
    EXECUTE 'UPDATE __tcache__ SET value = ' || $2
        || CASE WHEN $3 IS NULL THEN '' ELSE ', note = ' || quote_literal($3) END
        || ' WHERE label = ' || quote_literal($1);
    GET DIAGNOSTICS rcount = ROW_COUNT;
    IF rcount = 0 THEN
       RETURN tap._add( $1::text, $2::int, $3::text );
    END IF;
    RETURN $2;
END;
$function$
