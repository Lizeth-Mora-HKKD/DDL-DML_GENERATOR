CREATE OR REPLACE PROCEDURE PUBLIC.SP_MIGRATION_SCRIPT_EXTRACT_PROCEDURES("DB_NAME" VARCHAR(16777216), "SH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
declare
  PROCEDURE_type varchar default ''PROCEDURE'';
  SEQUENCE_type varchar default ''SEQUENCE'';
  tab_name varchar;
  qualified_tab_name varchar;
  Creat_tmp_sql varchar;
  Get_DDL_Sql varchar;
  res_trg_tab resultset default (select distinct PROCEDURE_CATALOG,PROCEDURE_SCHEMA,PROCEDURE_NAME,ARGUMENTS  from :sh.proc_parameters where PROCEDURE_CATALOG =:db_name and 
  PROCEDURE_SCHEMA =:sh );
  res_src_view resultset default (select distinct SEQUENCE_CATALOG,SEQUENCE_SCHEMA,SEQUENCE_NAME from information_schema.SEQUENCES where SEQUENCE_CATALOG=:db_name and SEQUENCE_SCHEMA=:sh);
  c1 cursor for res_trg_tab;
  c2 cursor for res_src_view;
  script resultset;
  insert_sql varchar;
  copy_st varchar;
  schema_name varchar;
  object_name varchar;
  object_type varchar;
  res_fold resultset;
  ext_stage_path varchar;
  col varchar default ''replace(sql,char(10),char(13))'';
  col2 varchar default ''OBJECT_TYPE'';
  proc_temp_table varchar;
  proc_argument varchar;
  show_proc varchar;

begin

--creating the temporary tables to store the dml statement
 proc_temp_table := ''create or replace TEMPORARY TABLE ''|| :db_name||''.''||:sh||''.proc_parameters (
	               PROCEDURE_CATALOG VARCHAR(100),
	               PROCEDURE_SCHEMA VARCHAR(100),
	               PROCEDURE_NAME VARCHAR(100),
	               ARGUMENTS VARCHAR(16777216)
                    );'';
 execute immediate :proc_temp_table;

show_proc := '' show procedures in schema ''||:sh;
execute immediate :show_proc;
select regexp_substr($9, '\\(.*?\\)') INTO :proc_argument from table(result_scan(last_query_id()));
proc_temp_table_insert := '' insert into ''||:sh|| ''.proc_parameters select '':proc_argument;
execute immediate :proc_temp_table_insert;

   --extracting all target tables DDLs 
  for row_variable in c1 do
    qualified_tab_name := row_variable.PROCEDURE_CATALOG||''.''||row_variable.PROCEDURE_SCHEMA||''.''||row_variable.PROCEDURE_NAME||row_variable.ARGUMENTS;
schema_name := row_variable.PROCEDURE_SCHEMA;
    tab_name  := row_variable.PROCEDURE_NAME;
    Get_DDL_Sql := ''insert into PUBLIC.DDL_TABLE_TEMP select ?,?,?,get_ddl(?,?,true) WHERE ? NOT IN (SELECT OBJECT_NAME FROM PUBLIC.DDL_TABLE_TEMP) AND ? <> ''''PROCEDURE'''''';
   script := (execute immediate :Get_DDL_Sql using (schema_name,PROCEDURE_type,tab_name,PROCEDURE_type,qualified_tab_name,tab_name,col2));
   end for;
   -- extracting all source views DDls
   for row_variable in c2 do
    qualified_tab_name := row_variable.SEQUENCE_CATALOG||''.''||row_variable.SEQUENCE_SCHEMA||''.''||row_variable.SEQUENCE_NAME;
schema_name := row_variable.SEQUENCE_SCHEMA;
    tab_name  := row_variable.SEQUENCE_NAME;
    Get_DDL_Sql := ''insert into PUBLIC.DDL_TABLE_TEMP select ?,?,?,get_ddl(?,?,true) WHERE ? NOT IN (SELECT OBJECT_NAME FROM PUBLIC.DDL_TABLE_TEMP) AND ? <> ''''SEQUENCE'''''';
   script := (execute immediate :Get_DDL_Sql using (schema_name,SEQUENCE_type,tab_name,SEQUENCE_type,qualified_tab_name,tab_name,col2));
   end for;
   -- writing all table DDLs to one SQL file on local folder. 
   res_fold := (select distinct SCHEMA,object_name,OBJECT_TYPE from PUBLIC.DDL_TABLE_TEMP);
   let c3 cursor for res_fold;
   for row_variable in c3 do
     object_name := row_variable.object_name;
     object_type := row_variable.OBJECT_TYPE;
     schema_name := row_variable.SCHEMA;
     ext_stage_path:= ''@DDL_UNLOAD_STAGE/''||:object_type||''_''||:schema_name||''_''||:object_name || ''.sql'';
     copy_st := ''copy into ''||ext_stage_path||'' from (select DISTINCT ''||col||'' from PUBLIC.DDL_TABLE_TEMP where object_name IN ( ''''''''''||:object_name||'''''''''') ) file_format = (format_name =SQL_TEXT_FORMAT compression = None) SINGLE = TRUE; '';
    --script:= (execute immediate :copy_st );
    col2 := ''insert into PUBLIC.STAGE_TABLE_TEMP SELECT ''''''||:copy_st||'''''''';
    execute immediate :col2;
    
   end for;
return 1;
end;
';
