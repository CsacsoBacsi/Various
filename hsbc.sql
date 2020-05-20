drop table udm.organisation ;
-- *** Setup data ***
-- UDM
create schema udm ;

create table udm.formation_type (formation_type_identifier serial, formation_type_code varchar (20) not null, name varchar (255), primary key (formation_type_identifier)) ;
create table udm.organisation (organisation_identifier serial, party_identifier int4 not null, formation_type_identifier int4, organisation_name varchar (255), formation_date date,
                               operation_start_date date, formation_identifier varchar (255), relationship_manager_code varchar (20), primary key (organisation_identifier)) ;
create table udm.party (party_identifier serial, party_type_identifier int4 not null, party_record_create_date date, primary key (party_identifier)) ;
create table udm.party_reference (party_reference_identifier serial, party_identifier int4 not null, party_reference_type int4 not null, reference_value varchar (255) not null,
                                  party_reference_start_date date, party_reference_end_date date, primary key (party_reference_identifier)) ;

alter table udm.organisation add constraint fk1 foreign key (party_identifier) references udm.party (party_identifier) ;
alter table udm.organisation add constraint fk2 foreign key (formation_type_identifier) references udm.formation_type (formation_type_identifier) ;
alter table udm.party_reference add constraint fk1 foreign key (party_identifier) references udm.party (party_identifier) ;

-- COH
create schema coh ;

create table coh.organisation (reference_value varchar (255), formation_type_code varchar (20), name varchar (255), organisation_name varchar (255),
             formation_date date, operation_start_date date, formation_identifier varchar (255), relationship_manager_code varchar (20)) ;

-- Coh inserts
insert into coh.organisation (reference_value, formation_type_code, name, organisation_name,
                              formation_date, operation_start_date, formation_identifier, relationship_manager_code)
values ('NKey_001', '001', 'Name1', 'OrgName_1', '2020-05-01'::date, '2020-05-10'::date, 'FormID_1', 'RelMGR_1') ;          
            
-- META
create schema meta ;

create table meta.mandatory_organisation (column_name varchar (63)) ; -- Issue: columns with same name in micro-domain tables 
create table meta.coh_udm_mapping_organisation (coh_column varchar (63), udm_table varchar (63), udm_column varchar (63)) ;

-- Meta inserts
insert into meta.mandatory_organisation (column_name) values ('organisation_name'), ('operation_start_date'), ('relationship_manager_code') ;
insert into meta.coh_udm_mapping_organisation (coh_column, udm_table, udm_column) 
     values ('reference_value', 'party_reference', 'reference_value'),
            ('formation_type_code', 'formation_type', 'formation_type_code'),
            ('name', 'formation_type', 'name'),
            ('organisation_name', 'organisation', 'organisation_name'),
            ('formation_date', 'organisation', 'formation_date'),
            ('operation_start_date', 'organisation', 'operation_start_date'),
            ('formation_identifier', 'organisation','formation_identifier'),
            ('relationship_manager_code', 'organisation', 'relationship_manager_code') ;

-- Procedures
CREATE OR REPLACE PROCEDURE coh.check_organisation (IN p_table_name text, IN p_gener bool, p_party_id text)
LANGUAGE plpgsql
AS $proc$

DECLARE

l_proc text ;
l_col text ;
l_row text ;
l_result bool ;

csr refcursor ;

BEGIN
    IF p_gener THEN -- Only re-generate function if p_gener flag set to True indicating metadata changes
    	OPEN csr for execute ('SELECT column_name FROM meta.mandatory_' || quote_ident (p_table_name)) ;
    	l_row := 'row (' ;
    	LOOP
        	FETCH csr INTO l_col ;
	    	EXIT WHEN NOT FOUND ;
	    	l_row := l_row || l_col || ', ' ; 
		END LOOP ;
    	CLOSE csr ;
    	l_row := substring (l_row, 1, length (l_row) - 2) ;
    	l_row := l_row || ')' ;
    	l_proc := 'create or replace function coh._check_organisation (p_party_id text)' || chr (10) ||
              	  'returns boolean' || chr (10) ||
                  'as $p$' || chr (10) ||
                  'declare' || chr (10) ||
                  '    l_query text ;' || chr (10) ||
                  '    l_result integer ;' || chr (10) ||
                  'begin' || chr (10) ||
                  '    l_query := ''select 1 from coh.' || quote_ident (p_table_name) || ' where reference_value = $1 and ' || l_row || ' is not null'';' || chr (10) ||
                  '    execute l_query into l_result using p_party_id ;' || chr (10) ||
                  '    if l_result is null then' || chr (10) ||
                  '        return false ;' || chr (10) ||
                  '    else' || chr (10) ||
                  '        return true ;' || chr (10) ||
                  '    end if ;' || chr (10) ||
                  'end ;' || chr (10) ||
                  '$p$ ' || chr (10) ||
                  'language plpgsql ;' || chr (10) ;
        EXECUTE l_proc ;
        RAISE NOTICE 'Proc created.' ;
       
        COMMIT ;
    END IF ;
    SELECT coh._check_organisation (p_party_id) INTO l_result ;
    RAISE NOTICE 'Check: %', l_result ;
END ;
$proc$ ;

call coh.check_organisation ('organisation', True, 'NKey_001') ;
select coh._check_organisation ('NKey_001') ;

CREATE OR REPLACE FUNCTION coh._check_organisation(p_party_id text)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    l_query text ;
    l_result integer ;
begin
    l_query := 'select 1 from coh.organisation where reference_value = $1 and row (organisation_name, operation_start_date, relationship_manager_code) is not null';
    execute l_query into l_result using p_party_id ;
    if l_result is null then
        return false ;
    else
        return true ;
    end if ;
end ;
$function$
;

