# SQL To Kafka
## Expediting the Refactoring of SQL Batch Queries to Streaming By Using an External DSL

Group By is the first SQL Operator is be implemented.

This application will perform a group by operation, similar to a group by in SQL.

There are currently 5 aggregate operations that group by will support

_`Sum`_ - This will sum up all values in a column.

_`Avg`_ - This will give the average of all values in a column.  The output will be in a decimal format.

_`Min`_ - This will find the minimum value in a column. This will handle numeric or date columns.

_`Max`_ - This will find the maximum value in a column. This will handle numeric or date columns.

_`Concat`_ - This will concat values together given a seperator. Default seperator is "||".

**Multi-Column Aggregations** - An infinite number of input columns can be aggregated.
The **ordinal** position of the 1 - input columns, which are given as _aggregation.columns_, the 2 - column aliases, which
are given as _aggregation.outputcolumns_, and the 3 - operations, which are given as _aggregation.operations_, are what 
determine the operation and alias that are applied to the column.

_`aggregation.dateformat`_ - **optional date format field** - it is good to provide the date format of the date fields.  This ensures the date will be parsed correctly.
If there is no date format given, most of common date formats will be parsed.

For the file format - the format has to be identical to below format.  

  This is an example file :
  
    groupBySteps: [ 
    {
    
      topic.in = "test-input-topic"
      topic.out = "groupby-orders"
      groupBy.columns = ["Col1"]
      aggregation.columns = ["Col2","DateCol3","DateCol4","Col5","Col6"]
      aggregation.outputcolumns = ["SumC2","MinC3","MaxC4","ConcatC5","AvgC6"]
      aggregation.operations = ["sum","min","max","concat","avg"]
      aggregation.dateformat = ["yyyy-MM-dd HH:mm:ss"]
      
      }
    ]


  Examples :
  
  **"sum" - input:** _`3`_, _`1`_, _`6`_  **output:** _`10.0`_
  
  **"sum" - input:** _`3.5`_, _`4.5`_, _`1.3`_  **output:** _`9.3`_
  
  **"avg" - input:** _`5`_, _`6`_, _`7`_  **output:** _`6.0`_
  
  **"avg" - input:** _`5`_, _`8`_, _`7`_, _`5`_   **output:** _`6.25`_
  
  **"min" - input:** _`'2018-10-19 18:00:00'`_, _`'2018-10-19 17:00:00'`_, _`'2018-10-20 18:00:00'`_  **output:** _`'2018-10-19 17:00:00'`_
  
  **"min" - input:** _`5`_, _`8`_, _`7`_  **output:** _`5`_
  
  **"max" - input:** _`'2018-10-19 18:00:00'`_, _`'2018-10-19 17:00:00'`_, _`'2018-10-20 18:00:00'`_  **output:** _`'2018-10-20 18:00:00'`_
  
  **"max" - input:** _`5`_, _`8`_, _`7`_  **output:** _`8`_
  
  **"concat" - input:** _`5`_, _`8`_, _`7`_  **output:** _`5||8||7`_