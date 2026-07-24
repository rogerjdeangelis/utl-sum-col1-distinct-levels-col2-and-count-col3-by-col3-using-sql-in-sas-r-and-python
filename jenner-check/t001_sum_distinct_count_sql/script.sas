/* SAS SQL solution from
   utl-sum-col1-distinct-levels-col2-and-count-col3-by-col3-using-sql-in-sas-r-and-python.sas
   The only change from the upstream script is that the input library
   (libname sd1 "d:/sd1") is dropped in favour of WORK, so the bundle is
   self-contained. The DATA step, its cards4 data, and the PROC SQL query
   are exactly as written upstream.

   Want, per color: distinct id count, sum of level, and total records.
   Expected: Blue 1/0/2, Green 2/1/2, Red 2/2/2. */

data have;
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

proc sql;

  create
     table want as
  select
     color
     ,count(distinct id) as ids
     ,sum(level)         as most_important
     ,count(*)           as records
  from
      have
  group
      by color

;quit;

proc print data=want;
run;quit;
