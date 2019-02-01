echo "select * from table1"
python mycode.py "select * from table1"
echo "select * from table1;"
python mycode.py "select * from table1;"

echo "select max(A) from table1;"
python mycode.py "select max(A) from table1;"

echo "select a,b from table1;"
python mycode.py "select a,b from table1;"

echo "select b,d from table2;"
python mycode.py "select b,d from table2;"

echo "select distinct b,d from table2;"
python mycode.py "select distinct b,d from table2;"

echo "select * from table2 where b=811 and d=13393 ;"
python mycode.py "select * from table2 where b=811 and d=13393 ;"

echo "select * from table1 where a>858;"
python mycode.py "select * from table1 where a>858;"

echo "select * from table1 where a>=858;"
python mycode.py "select * from table1 where a>=858;"

echo "select * from table1 where a>858 or b<200;"
python mycode.py "select * from table1 where a>858 or b<200;"

echo "select * from table2 where b=646;"
python mycode.py "select * from table2 where b=646;"

echo "select distinct * from table2 where b=646;"
python mycode.py "select distinct * from table2 where b=646;"

echo "select min(b) from table2 where b>646;"
python mycode.py "select min(b) from table2 where b>646;"

echo "select min(b) from table1,table2 where b>646;"
python mycode.py "select min(b) from table1,table2 where b>646;"

echo "select min(table2.b) from table1,table2 where table2.b>646;"
python mycode.py "select min(table2.b) from table1,table2 where table2.b>646;"

echo "select a,table2.b,c from table1,table2 where c<1600 and table2.b<100;"
python mycode.py "select a,table2.b,c from table1,table2 where c<1600 and table2.b<100;"

echo "select * from table1,table2 where table1.B=table2.b;"
python mycode.py "select * from table1,table2 where table1.B=table2.b;"

echo "select a,b table1,table2 where table1.B=table2.b;"
python mycode.py "select a,b table1,table2 where table1.B=table2.b;"

echo "select a,b from table1,table2 where table1.B=table2.b;"
python mycode.py "select a,b from table1,table2 where table1.B=table2.b;"

echo "select a,table1.b,d from table1,table2 where table1.B=table2.b;"
python mycode.py "select a,table1.b,d from table1,table2 where table1.B=table2.b;"

echo "select a,table1.b,d from table1,table2 where table1.B>table2.b;"
python mycode.py "select a,table1.b,d from table1,table2 where table1.B>table2.b;"

