### Build



### Run

> create hbase table

```shell
hbase> crete_namespace 'benchmark'
hbase> create 'benchmark:dmbout', 'f'
```



```shell
hadoop jar scala-hbase-app-1.0.0-jar-with-dependencies.jar /benchmark/tmp/dmbout/year=2019/month=01/000000_0 benchmark:dmbout
```





