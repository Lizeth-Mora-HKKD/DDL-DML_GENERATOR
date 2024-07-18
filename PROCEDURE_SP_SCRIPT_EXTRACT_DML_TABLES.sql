CREATE OR REPLACE PROCEDURE PUBLIC.SP_MIGRATION_SCRIPT_EXTRACT("DB_NAME" VARCHAR(16777216), "SH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
declare
  table_type varchar default ''TABLE'';
  view_type varchar default ''VIEW'';
  tab_name varchar;
  databasename varchar;
  qualified_tab_name varchar;
  Creat_tmp_sql varchar;
  Get_DDL_Sql varchar;
  Get_DML_Sql varchar;
  res_trg_tab resultset default (select distinct top 1 TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME from information_schema.tables where TABLE_CATALOG=:db_name and TABLE_SCHEMA=:sh AND table_type = 
  ''BASE TABLE'' );
  res_src_view resultset default (select distinct TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME from information_schema.views where TABLE_CATALOG=:db_name and TABLE_SCHEMA=:sh);
  c1 cursor for res_trg_tab;
  c2 cursor for res_src_view;
  script resultset;
  dml_script varchar;
  insert_sql varchar;
  copy_st varchar;
  schema_name varchar;
  object_name varchar;
  object_type varchar;
  res_fold resultset;
  ext_stage_path varchar;
  col varchar default ''replace(sql,char(10),char(13))'';
  col2 varchar default ''OBJECT_NAME'';
begin

   --Build the db name from the variable in the sp
        select split_part(:db_name,''_'',2) into :databasename;
   --extracting all target tables DDLs 
  for row_variable in c1 do
    qualified_tab_name := row_variable.TABLE_CATALOG||''.''||row_variable.TABLE_SCHEMA||''.''||row_variable.TABLE_NAME;
schema_name := row_variable.TABLE_SCHEMA;
    tab_name  := row_variable.TABLE_NAME;
    --extracting the insert into DML
    Get_DML_Sql := ''select ''''
    
--************** Alliant Energy *****************-- 
--** Author: Liz Mora (Hakkoda)		Initial creation
--** Date: June 2024
--** Description: DML table structure 
--*******************************************************************************************


INSERT OVERWRITE INTO &{env}_''||:databasename||''.TRANSFORM.''||:tab_name ||'' Select '''' || array_to_string(array_agg(
    CASE 
    WHEN data_type = ''''TEXT'''' THEN  ''''NULLIF(UPPER(TRIM(''''|| column_name || '''')), '''''''''''''''''''''''''''''''' ) as '''' ||column_name 
    ELSE  column_name || '''' as ''''||column_name  
    END  ) within group (order by ordinal_position),'''' , '''')||'''' FROM &{env}_''||:databasename||''.''||:schema_name||''.''||:tab_name||'';''''
from information_schema.columns
where table_schema = ''''''||:schema_name ||'''''' and table_name ='''''' ||:tab_name ||'''''';''
;

execute immediate :Get_DML_Sql;
select * INTO :dml_script from table(result_scan(last_query_id()));
--dml_script := (execute immediate :Get_DML_Sql );

    Get_DDL_Sql := ''insert into  PUBLIC.DML_TABLE_TEMP select ?,?,?,''''''||:dml_script||'''''' WHERE ? NOT IN (SELECT OBJECT_NAME FROM PUBLIC.DML_TABLE_TEMP )'';
  script := (execute immediate :Get_DDL_Sql using (schema_name,table_type,tab_name,table_type,qualified_tab_name,tab_name));
   end for;
   -- extracting all source views DDls
 /*  for row_variable in c2 do
    qualified_tab_name := row_variable.TABLE_CATALOG||''.''||row_variable.TABLE_SCHEMA||''.''||row_variable.TABLE_NAME;
schema_name := row_variable.TABLE_SCHEMA;
    tab_name  := row_variable.TABLE_NAME;
    Get_DDL_Sql := ''insert into PUBLIC.DML_TABLE_TEMP select ?,?,?,get_ddl(?,?,true) WHERE ? NOT IN (SELECT OBJECT_NAME FROM PUBLIC.DML_TABLE_TEMP)'';
   script := (execute immediate :Get_DDL_Sql using (schema_name,view_type,tab_name,view_type,qualified_tab_name,tab_name));
   end for;*/
   -- writing all table DDLs to one SQL file on local folder. 
   res_fold := (select distinct SCHEMA,object_name,OBJECT_TYPE from PUBLIC.DML_TABLE_TEMP);
   let c3 cursor for res_fold;
   for row_variable in c3 do
     object_name := row_variable.object_name;
     object_type := row_variable.OBJECT_TYPE;
     schema_name := row_variable.SCHEMA;
     ext_stage_path:= ''@DML_UNLOAD_STAGE/DWH_''||:DB_NAME||''_FULL_TARGETDB_''||:object_name || ''.sql'';
     copy_st := ''copy into ''||ext_stage_path||'' from (select ''||col||'' from PUBLIC.DML_TABLE_TEMP where object_name IN ( ''''''''''||:object_name||'''''''''') ) file_format = (format_name =SQL_TEXT_FORMAT compression = None) SINGLE = TRUE; '';
    --script:= (execute immediate :copy_st );
    col2 := ''insert into  PUBLIC.STAGE_TABLE_TEMP SELECT ''''''||:copy_st||'''''''';
    execute immediate :col2;
    
   end for;
return 1;
end;
';