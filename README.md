### Build



### Run

> create hbase table

```shell
hbase> create_namespace 'benchmark'
hbase> create 'benchmark:dmbout', 'f'
```

```shell
hbase> create_namespace 'benchmark'
hbase> create 'benchmark:paybill', 'b', 'd'
```



```shell
hadoop jar scala-hbase-app-1.0.0-jar-with-dependencies.jar /benchmark/tmp/dmbout/year=2019/month=01/000000_0 benchmark:dmbout
```





