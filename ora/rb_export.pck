create or replace package RB_EXPORT is

  -- Author  : Qvant86
  -- Created : 17.04.2017 0:21:09
  -- Purpose : Simple package for save query result into csv
  
  -- Get package version
  function getVersion return number deterministic;
  
  --Save query result 
  --%param pCur ref cursor for export
  --%param pDirName Oracle dir for saving files. Must exists
  --%param pSeparator Field separator
  --%param pQuotas Quota sign
  --%param pQuoteStr enquote all strings
  --%param pQuoteAll enquote all fields
  --%param pNumFormat standart Oracle format string for number to char conversion
  --%param pDateFormat standart Oracle format string for date to char conversion
  --%param pNoNewLines remove all new lines from the field values
  procedure refCursorToCsv(pCur        in out sys_refcursor,
                           pDirName    in varchar2,
                           pFileName   in varchar2,
                           pSeparator  in varchar2 default ',',
                           pEscape     in varchar2 default null,
                           pQuotas     in varchar2 default '"',
                           pQuoteStr   in boolean default false,
                           pQuoteAll   in boolean default false,
                           pNumFormat  in varchar2 default 'FM99999999999999999999999999999.00000PR',
                           pDateFormat in varchar2 default 'dd.mm.yyyy hh24:mi:ss',
                           pNoNewLines in boolean default false);
  
  -- Save dynamic sql result
  --%param pQuery dynamic sql for export
  --%param pDirName Oracle dir for saving files. Must exists
  --%param pSeparator Field separator
  --%param pQuotas Quota sign
  --%param pQuoteStr enquote all strings
  --%param pQuoteAll enquote all fields
  --%param pNumFormat standart Oracle format string for number to char conversion
  --%param pDateFormat standart Oracle format string for date to char conversion
  --%param pNoNewLines remove all new lines from the field values                         
  procedure queryToCsv(pQuery      in varchar2,
                       pDirName    in varchar2,
                       pFileName   in varchar2,
                       pSeparator  in varchar2 default ',',
                       pEscape     in varchar2 default null,
                       pQuotas     in varchar2 default '"',
                       pQuoteStr   in boolean default false,
                       pQuoteAll   in boolean default false,
                       pNumFormat  in varchar2 default 'FM99999999999999999999999999999.00000PR',
                       pDateFormat in varchar2 default 'dd.mm.yyyy hh24:mi:ss',
                       pNoNewLines in boolean default false);

end RB_EXPORT;
/
create or replace package body RB_EXPORT is

  c_version      constant number := 1;
  c_type_number  constant number := 12;
  c_type_date    constant number := 12;
  c_max_linesize constant number := 32767;
  c_file_open_mode constant varchar2(1 char) := 'w';

  function getVersion return number is
  begin
    return c_version;
  end;

  procedure refCursorToCsv(pCur        in out sys_refcursor,
                           pDirName    in varchar2,
                           pFileName   in varchar2,
                           pSeparator  in varchar2,
                           pEscape     in varchar2,
                           pQuotas     in varchar2,
                           pQuoteStr   in boolean,
                           pQuoteAll   in boolean,
                           pNumFormat  in varchar2,
                           pDateFormat in varchar2,
                           pNoNewLines in boolean) is
    l_file      utl_file.file_type;
    l_cur       number;
    l_col_cnt   number;
    l_col_desc  dbms_sql.desc_tab;
    l_str_buf   VARCHAR2(4000); --Think about 12c+ MAX_STRING_SIZE initualization param
    l_num_buf   NUMBER;
    l_date_buf  DATE;
    l_first_row boolean := true;
  begin
    l_file := utl_file.fopen(location     => pDirName,
                             filename     => pFileName,
                             open_mode    => c_file_open_mode,
                             max_linesize => c_max_linesize);
    l_cur  := dbms_sql.to_cursor_number(rc => pCur);
    dbms_sql.describe_columns(c       => l_cur,
                              col_cnt => l_col_cnt,
                              desc_t  => l_col_desc);
  
    for i in 1 .. l_col_cnt loop
      if l_col_desc(i).col_type = c_type_number then
        dbms_sql.define_column(l_cur, i, l_num_buf);
      elsif l_col_desc(i).col_type = c_type_date then
        dbms_sql.define_column(l_cur, i, l_date_buf);
      else
        dbms_sql.define_column(l_cur, i, l_str_buf, 4000);
      end if;
    end loop;
    while dbms_sql.fetch_rows(l_cur) > 0 loop
      if l_first_row then
        l_first_row := false;
      else
        utl_file.put_line(l_file, '');
      end if;
      for i in 1 .. l_col_cnt loop
        if l_col_desc(i).col_type = c_type_number then
          dbms_sql.column_value(l_cur, i, l_num_buf);
          l_str_buf := to_char(l_num_buf, pNumFormat);
        elsif l_col_desc(i).col_type = c_type_date then
          dbms_sql.column_value(l_cur, i, l_date_buf);
          l_str_buf := to_char(l_date_buf, pDateFormat);
        else
          dbms_sql.column_value(l_cur, i, l_str_buf);
        end if;
        if pNoNewLines then
          l_str_buf := replace(replace(l_str_buf, chr(10), ''), chr(13), '');
        end if;
        if instr(l_str_buf, pSeparator) > 0 then
          utl_file.put(file   => l_file,
                       buffer => pQuotas ||
                                 replace(l_str_buf,
                                         pSeparator,
                                         pEscape || pSeparator) || pQuotas ||
                                 pSeparator);
        else
          if (pQuoteAll and l_col_desc(i)
             .col_type in (c_type_number, c_type_date)) or pQuoteStr then
            utl_file.put(file   => l_file,
                         buffer => pQuotas || l_str_buf || pQuotas ||
                                   pSeparator);
          else
            utl_file.put(file => l_file, buffer => l_str_buf || pSeparator);
          end if;
        end if;
      end loop;
    end loop;
    utl_file.fclose(l_file);
    dbms_sql.close_cursor(l_cur);
  end;

  procedure queryToCsv(pQuery      in varchar2,
                       pDirName    in varchar2,
                       pFileName   in varchar2,
                       pSeparator  in varchar2,
                       pEscape     in varchar2,
                       pQuotas     in varchar2,
                       pQuoteStr   in boolean,
                       pQuoteAll   in boolean,
                       pNumFormat  in varchar2,
                       pDateFormat in varchar2,
                       pNoNewLines in boolean) is
  
    l_ref sys_refcursor;
  
  begin
  
    open l_ref for pQuery;
    refCursorToCsv(pCur        => l_ref,
                   pDirName    => pDirName,
                   pFileName   => pFileName,
                   pSeparator  => pSeparator,
                   pEscape     => pEscape,
                   pQuotas     => pQuotas,
                   pQuoteStr   => pQuoteStr,
                   pQuoteAll   => pQuoteAll,
                   pNumFormat  => pNumFormat,
                   pDateFormat => pDateFormat,
                   pNoNewLines => pNoNewLines);
  
  end;

end RB_EXPORT;
/
