# utl-sum-col1-distinct-levels-col2-and-count-col3-by-col3-using-sql-in-sas-r-and-python
Sum col1 distinct levels col2 and count col3 by col3 using sql in sas r and python   
    %let pgm=utl-sum-col1-distinct-levels-col2-and-count-col3-by-col3-using-sql-in-sas-r-and-python;

    Sum col1 distinct levels col2 and count col3 by col3 using sql in sas r and python

     Three Solutions
         1 sas sql
         2 r sql
         3 python sql
         4 dplyr r


    github
    https://tinyurl.com/ydpnpemt
    https://github.com/rogerjdeangelis/utl-sum-col1-distinct-levels-col2-and-count-col3-by-col3-using-sql-in-sas-r-and-python

    stackoverflow
    https://tinyurl.com/yn7sz7zn
    https://stackoverflow.com/questions/78954754/r-counting-the-number-of-groups-having-each-value-of-a-variable

    /*               _     _
     _ __  _ __ ___ | |__ | | ___ _ __ ___
    | `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
    | |_) | | | (_) | |_) | |  __/ | | | | |
    | .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
    |_|
    */


    /**************************************************************************************************************************/
    /*                                            |                                          |                                */
    /*                                            |                                          |                                */
    /*          INPUT                             | PROCESS(self explanatory)                |     OUTPUT                     */
    /*                                            |                                          |                                */
    /*                                            |                                          |                                */
    /*   Obs    LEVEL     ID    COLOR             | SAME CODE IN SAS R and PYTHON            | WANT total obs=3               */
    /*                                            |                                          |                                */
    /*    1       0      101    Blue              | select                                   |             MOST_              */
    /*    2       0      101    Blue              |    color                                 | COLOR IDS IMPORTANT  RECORDS   */
    /*    3       1      101    Red               |    ,count(distinct id) as ids            |                                */
    /*    4       1      102    Red               |    ,sum(level)         as most_important | Blue   1      0         2      */
    /*    5       0      102    Green             |    ,count(*)           as records        | Green  2      1         2      */
    /*    6       1      103    Green             | from                                     | Red    2      2         2      */
    /*                                            |     sd1.have                             |                                */
    /*                                            | group                                    |                                */
    /*                                            |     by color                             |                                */
    /*                                            |                                          |                                */
    /*                                            |---------------------------------------------------------------------------*/
    /*                                            |                                          |                                */
    /*                                            | R                                        |                                */
    /*                                            | =                                        |                                */
    /*                                            |                                          |                                */
    /*                                            | want<-df_colors |>                       |                                */
    /*                                            |  group_by(COLOR) %>%                     |                                */
    /*                                            |  summarise(                              |                                */
    /*                                            |    ids = n_distinct(ID),                 |                                */
    /*                                            |    most_important =                      |                                */
    /*                                            |     n_distinct(if_else(LEVEL, ID         |                                */
    /*                                            |        ,NA_character_)                   |                                */
    /*                                            |       ,na.rm = T)                        |                                */
    /*                                            |       ,records = n()                     |                                */
    /*                                            |   )                                      |                                */
    /*                                            |                                          |                                */
    /*                                            |   Little odd                             |                                */
    /*                                            |                                          |                                */
    /*                                            |    most_important =                      |                                */
    /*                                            |     n_distinct(if_else(LEVEL             |                                */
    /*                                            |      ,ID,NA_character_)                  |                                */
    /*                                            |      ,na.rm = T)                         |                                */
    /*                                            |                                          |                                */
    /**************************************************************************************************************************/

    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */

    options validvarname=upcase;
    libname sd1 "d:/sd1";
    data sd1.have;
     input level 2. id $3. color $;
    cards4;
    0 101 Blue
    0 101 Blue
    1 101 Red
    1 102 Red
    0 102 Green
    1 103 Green
    ;;;;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  Obs    LEVEL     ID    COLOR                                                                                          */
    /*                                                                                                                        */
    /*   1       0      101    Blue                                                                                           */
    /*   2       0      101    Blue                                                                                           */
    /*   3       1      101    Red                                                                                            */
    /*   4       1      102    Red                                                                                            */
    /*   5       0      102    Green                                                                                          */
    /*   6       1      103    Green                                                                                          */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*                             _
    / |  ___  __ _ ___   ___  __ _| |
    | | / __|/ _` / __| / __|/ _` | |
    | | \__ \ (_| \__ \ \__ \ (_| | |
    |_| |___/\__,_|___/ |___/\__, |_|
                                |_|
    */

    proc sql;

      create
         table want as
      select
         color
         ,count(distinct id) as ids
         ,sum(level)         as most_important
         ,count(*)           as records
      from
          sd1.have
      group
          by color

    ;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  WANT total obs=3                                                                                                      */
    /*                      MOST_                                                                                             */
    /*    COLOR    IDS    IMPORTANT    RECORDS                                                                                */
    /*                                                                                                                        */
    /*    Blue      1         0           2                                                                                   */
    /*    Green     2         1           2                                                                                   */
    /*    Red       2         2           2                                                                                   */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    proc datasets lib=sd1 nodetails nolist;
     delete rwant;
    run;quit;

    %utl_rbeginx;
    parmcards4;
    library(haven);
    library(sqldf)
    source("c:/oto/fn_tosas9x.R");
    df_colors<-read_sas("d:/sd1/have.sas7bdat");
    df_colors
    want<-sqldf('
      select
         color
         ,count(distinct id) as ids
         ,sum(level)         as most_important
         ,count(*)           as records
      from
          df_colors
      group
          by color
      ')
    want;
    want
    fn_tosas9x(
          inp    = want
         ,outlib ="d:/sd1/"
         ,outdsn ="rwant"
         );
    ')
    ;;;;
    %utl_rendx;

    proc print data=sd1.rwant;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  RWANT total obs=3                                                                                                     */
    /*                                                                                                                        */
    /*                                 MOST_                                                                                  */
    /*   ROWNAMES    COLOR    IDS    IMPORTANT    RECORDS                                                                     */
    /*                                                                                                                        */
    /*       1       Blue      1         0           2                                                                        */
    /*       2       Green     2         1           2                                                                        */
    /*       3       Red       2         2           2                                                                        */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*____               _   _                             _
    |___ /   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
      |_ \  | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
     ___) | | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
    |____/  | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
            |_|    |___/                                |_|
    */
    %utl_pybeginx;
    parmcards4;
    import pyperclip
    import os
    from os import path
    import sys
    import subprocess
    import time
    import pandas as pd
    import pyreadstat as ps
    import numpy as np
    import pandas as pd
    from pandasql import sqldf
    mysql = lambda q: sqldf(q, globals())
    from pandasql import PandaSQL
    pdsql = PandaSQL(persist=True)
    sqlite3conn = next(pdsql.conn.gen).connection.connection
    sqlite3conn.enable_load_extension(True)
    sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
    mysql = lambda q: sqldf(q, globals())
    have, meta = ps.read_sas7bdat("d:/sd1/have.sas7bdat")
    exec(open('c:/temp/fn_tosas9.py').read())
    print(have);
    want = pdsql("""
      select
         color
         ,count(distinct id) as ids
         ,sum(level)         as most_important
         ,count(*)           as records
      from
          have
      group
          by color
    """)
    print(want)
    fn_tosas9(
       want
       ,dfstr="want"
       ,timeest=3
       )
    ;;;;
    %utl_pyendx;

    libname tmp "c:/temp";
    proc print data=tmp.want;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  PYTHON                                                                                                                */
    /*                                                                                                                        */
    /*     COLOR  ids  most_important  records                                                                                */
    /*  0   Blue    1             0.0        2                                                                                */
    /*  1  Green    2             1.0        2                                                                                */
    /*  2    Red    2             2.0        2                                                                                */
    /*                                                                                                                        */
    /*  SAS                                                                                                                   */
    /*                                                                                                                        */
    /* WANT total obs=3                                                                                                       */
    /*                                                                                                                        */
    /*                          MOST_                                                                                         */
    /* Obs    COLOR    IDS    IMPORTANT    RECORDS                                                                            */
    /*                                                                                                                        */
    /*  1     Blue      1         0           2                                                                               */
    /*  2     Green     2         1           2                                                                               */
    /*  3     Red       2         2           2                                                                               */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*  _         _       _
    | || |     __| |_ __ | |_   _ _ __
    | || |_   / _` | `_ \| | | | | `__|
    |__   _| | (_| | |_) | | |_| | |
       |_|    \__,_| .__/|_|\__, |_|
                   |_|      |___/
    */

    %utl_rbeginx;
    parmcards4;
    library(dplyr)
    df_colors <- tibble::tibble(
      id = c("101", "101", "101", "102", "102", "103"),
      color = c("Blue", "Blue", "Red", "Red", "Green", "Green"),
      most_important_color = c(F, F, T, T, F, T)
    )
    df_colors |>
      group_by(color) %>%
      summarise(
        ids = n_distinct(id),
        most_important = n_distinct(if_else(most_important_color, id, NA_character_), na.rm = T),
        records = n()
      )
    ;;;;
    %utl_rendx;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  # A tibble: 3 Ã— 4                                                                                                    */
    /*    color   ids most_important records                                                                                  */
    /*    <chr> <int>          <int>   <int>                                                                                  */
    /*  1 Blue      1              0       2                                                                                  */
    /*  2 Green     2              1       2                                                                                  */
    /*  3 Red       2              2       2                                                                                  */
    /*                                                                                                                        */

    /**************************************************************************************************************************/
    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */


