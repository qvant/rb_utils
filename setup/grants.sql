-- run as user SYS:

grant execute on sys.utl_file to &&your_schema;
grant execute on sys.dbms_sql to &&your_schema;
grant execute on sys.dbms_lob to &&your_schema;
grant execute on sys.dbms_crypto to &&your_schema;