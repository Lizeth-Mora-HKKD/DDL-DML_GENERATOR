# DML and DDL Dynamic Generator

This asset will help with the generation of the DML and DDL dynamically for all the tables/views/procedures/sequences that exist in information_schema.tables, you can also modify the query to filter by the needed table names.

This project will generate one file per table/view/procedure/sequence, each file will contain the DDL or DML.

To be able to use these project the stored procedure will create few TRANSIENT tables, internal stage and have a connection to Snowflake using Visual Code to be able to download the files from the internal stage to your local. The project steps are, generate the DML or DDL, store the script in a TRANSIENT table, grab the info in that table and generate a COPY INTO statement, then the stored procedure will run all the COPY INTO statements to insert all the DDL or DML in the internal stage and then download those to your local folder using the visual code connection, snowsql also works.



## Deployment

The process will consist in the creation of two tables, one internal stage and one file format. The tables will be TRANSIENT tables to store the DDL/DML code and the other one to store the copy into command to insert in the internal stage. 

Depends on the type of script that you need to generate will need some tables/views/sequences not all of them you can put where statement for select only the required ones. 

Before to start you will need to identify what you will need create DML only, DDL only or both.

To generate DDL: You will need to decide if your DDL include TABLES, VIEWS, STORED PROCEDURES or SEQUENCES.

⚡️ DDL TABLES AND VIEWS:

[Tables and Views Stored Procedure](https://github.com/lizmoramena/DML-DDL_SCRIPT_GENERATION/blob/main/PROCEDURE_SP_SCRIPT_EXTRACT_DDL_PROCEDURES_SEQUENCES.sql)

⚡️ DDL Procedures AND Sequences:

[Procedures and Sequences Stored Procedure](https://github.com/lizmoramena/DML-DDL_SCRIPT_GENERATION/blob/main/PROCEDURE_SP_SCRIPT_EXTRACT_DDL_PROCEDURES_SEQUENCES.sql)


To generate DML: Only available for tables.

⚡️ DML Table:

[Tables DML Stored Procedure](https://github.com/lizmoramena/DML-DDL_SCRIPT_GENERATION/blob/main/PROCEDURE_SP_SCRIPT_EXTRACT_DML_TABLES.sql)



## Authors

- [@lizmoramena](https://www.github.com/lizmoramena)

