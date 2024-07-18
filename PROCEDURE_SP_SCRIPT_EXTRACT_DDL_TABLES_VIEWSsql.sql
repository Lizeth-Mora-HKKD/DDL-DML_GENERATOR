CREATE OR REPLACE PROCEDURE PUBLIC.SP_MIGRATION_SCRIPT_EXTRACT("DB_NAME" VARCHAR(16777216), "SH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
declare
  table_type varchar default ''TABLE'';
  view_type varchar default ''VIEW'';
  tab_name varchar;
  qualified_tab_name varchar;
  Creat_tmp_sql varchar;
  Get_DDL_Sql varchar;
  res_trg_tab resultset default (select distinct TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME from information_schema.tables where TABLE_CATALOG=:db_name and TABLE_SCHEMA=:sh AND table_type = 
  ''BASE TABLE'' );
  res_src_view resultset default (select distinct TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME from information_schema.views where TABLE_CATALOG=:db_name and TABLE_SCHEMA=:sh);
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
  col2 varchar default ''OBJECT_NAME'';
  ddl_stage_temp_table varchar;
  ddl_temp_table varchar;
  create_file_format varchar;
  create_stage varchar;
  copy_into_stage_fold resultset;
  copy_into_statement_query varchar;
  copy_into_statement varchar;
  copy_into_quote varchar default ''replace(COPY_STATEMENT,'''''''''''''''''''''''','''''''''''''''')'';



begin

create_stage := ''CREATE STAGE IF NOT EXISTS "''||:db_name||''"."''||:sh||''"."DML_UNLOAD_STAGE"'';

    execute immediate :create_stage;

    create_file_format := ''CREATE FILE FORMAT IF NOT EXISTS "''||:db_name||''"."''||:sh||''".SQL_TEXT_FORMAT COMPRESSION = ''''AUTO'''' 
    FIELD_DELIMITER = ''''NONE'''' RECORD_DELIMITER = ''''
'''' 
     SKIP_HEADER = 0 FIELD_OPTIONALLY_ENCLOSED_BY = ''''NONE'''' 
     TRIM_SPACE = FALSE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE ESCAPE = ''''NONE'''' DATE_FORMAT = ''''AUTO'''' TIMESTAMP_FORMAT = ''''AUTO'''' NULL_IF = (''''\\\\N'''')
    '';

    execute immediate :create_file_format;

    
    remove @DML_UNLOAD_STAGE;

--creating the temporary tables to store the dml statement
 ddl_temp_table := ''create or replace TEMPORARY TABLE ''|| :db_name||''.''||:sh||''.DDL_TABLE_TEMP (
	               SCHEMA VARCHAR(100),
	               OBJECT_TYPE VARCHAR(100),
	               OBJECT_NAME VARCHAR(100),
	               SQL VARCHAR(16777216)
                    );'';
 execute immediate :ddl_temp_table;

 --creating the temporary tables to store the copy into statement
 ddl_stage_temp_table := ''create or replace TEMPORARY TABLE ''|| :db_name||''.''||:sh||''.STAGE_TABLE_TEMP (
	            COPY_STATEMENT VARCHAR(16777216)
                );'';

 execute immediate :ddl_stage_temp_table;

   --extracting all target tables DDLs 
  for row_variable in c1 do
    qualified_tab_name := row_variable.TABLE_CATALOG||''.''||row_variable.TABLE_SCHEMA||''.''||row_variable.TABLE_NAME;
schema_name := row_variable.TABLE_SCHEMA;
    tab_name  := row_variable.TABLE_NAME;
    Get_DDL_Sql := ''insert into ''||:sh||''.DDL_TABLE_TEMP select ?,?,?,get_ddl(?,?,true) WHERE ? NOT IN (SELECT OBJECT_NAME FROM ''||:sh||''.DDL_TABLE_TEMP )'';
   script := (execute immediate :Get_DDL_Sql using (schema_name,table_type,tab_name,table_type,qualified_tab_name,tab_name));
   end for;
   -- extracting all source views DDls
   for row_variable in c2 do
    qualified_tab_name := row_variable.TABLE_CATALOG||''.''||row_variable.TABLE_SCHEMA||''.''||row_variable.TABLE_NAME;
schema_name := row_variable.TABLE_SCHEMA;
    tab_name  := row_variable.TABLE_NAME;
    Get_DDL_Sql := ''insert into ''||:sh||''.DDL_TABLE_TEMP select ?,?,?,get_ddl(?,?,true) WHERE ? NOT IN (SELECT OBJECT_NAME FROM ''||:sh||''.DDL_TABLE_TEMP)'';
   script := (execute immediate :Get_DDL_Sql using (schema_name,view_type,tab_name,view_type,qualified_tab_name,tab_name));
   end for;
   -- writing all table DDLs to one SQL file on local folder. 
   res_fold := (select distinct SCHEMA,object_name,OBJECT_TYPE from ''||:sh||''.DDL_TABLE_TEMP);
   let c3 cursor for res_fold;
   for row_variable in c3 do
     object_name := row_variable.object_name;
     object_type := row_variable.OBJECT_TYPE;
     schema_name := row_variable.SCHEMA;
     ext_stage_path:= ''@DDL_UNLOAD_STAGE/''||:object_type||''_''||:schema_name||''_''||:object_name || ''.sql'';
     copy_st := ''copy into ''||ext_stage_path||'' from (select ''||col||'' from ''||:sh||''.DDL_TABLE_TEMP where object_name IN (''''''''''''''''''||:object_name|| '''''''''''''''''') ) file_format = (format_name =SQL_TEXT_FORMAT compression = None) SINGLE = TRUE; '';
    --script:= (execute immediate :copy_st );
    col2 := ''insert into ''||:sh||''.STAGE_TABLE_TEMP SELECT ''''''||:copy_st||'''''''';
    execute immediate :col2;
    
   end for;


   copy_into_stage_fold := (select distinct COPY_STATEMENT from STAGE_TABLE_TEMP);
let c4 cursor for copy_into_stage_fold;
for row_variable in c4 do
copy_into_statement_query := replace(row_variable.COPY_STATEMENT,'''''''''''''''''''''''','''''''''''''''');
  copy_into_statement := ''select ''||:copy_into_quote||'' from ''||:sh||''.STAGE_TABLE_TEMP where SPLIT_PART(''||:copy_into_quote||'','''' '''',3) = SPLIT_PART(''''''||:copy_into_statement_query||'''''','''' '''',3) ;'';
   execute immediate :copy_into_statement;
   select * INTO :insert_into_stage from table(result_scan(last_query_id()));
    execute immediate :insert_into_stage;
   end for;
return 1;
end;
';
