```sh
%sh

#remove existing copies of dataset from HDFS
hadoop fs -rm  /tmp/expenses.csv

#fetch the dataset
wget https://data.gov.au/dataset/f84b9baf-c1c1-437c-8c1e-654b2829848c/resource/88399d53-d55c-466c-8f4a-6cb965d24d6d/download/healthexpenditurebyareaandsource.csv -O /tmp/expenses.csv

#remove header
sed -i '1d' /tmp/expenses.csv
#remove empty fields
sed -i "s/,,,,,//g" /tmp/expenses.csv
sed -i '/^\s*$/d' /tmp/expenses.csv

#put data into HDFS
hadoop fs -put /tmp/expenses.csv /tmp
hadoop fs -ls -h /tmp/expenses.csv
rm /tmp/expenses.csv
```

```scala
val dataset=sc.textFile("/tmp/expenses.csv")
dataset.count()
dataset.first()
```

```scala
case class Health (year:  String, state: String, category:String, funding_src1: String, funding_scr2: String, spending: Integer)
val health = dataset.map(k=>k.split(",")).map(
    k => Health(k(0),k(1),k(2),k(3), k(4), k(5).toInt)
    )
// toDF() works only in spark 1.3.0.
// For spark 1.1.x and spark 1.2.x,
// use below instead:
// health.registerTempTable("health_table")
health.toDF().registerTempTable("health_table")
```

```sql
%sql
select state, sum(spending)/1000 SpendinginBillions 
from health_table 
group by state 
order by SpendinginBillions desc
```

```sql
%sql
select category, sum(spending)/1000 SpendinginBillions 
from health_table 
group by category 
order by SpendinginBillions desc
```