
1. **IF-THEN-ELSE:**
   ```java
   // Oracle/PLSQL
   // IF condition THEN statements1; ELSE statements2; END IF;
   
   // Spark SQL on Java
   Column condition = df.col("condition");
   Column result = functions.when(condition, df.col("statements1")).otherwise(df.col("statements2"));
   Dataset<Row> transformedDF = df.withColumn("result_column", result);
   ```

2. **SUM():**
   ```java
   // Oracle/PLSQL
   // SELECT SUM(column_name) FROM table_name;
   
   // Spark SQL on Java
   Dataset<Row> result = df.agg(functions.sum("column_name"));
   ```

3. **TO_NUMBER():**
   ```java
   // Oracle/PLSQL
   // TO_NUMBER(column_name);
   
   // Spark SQL on Java
   Dataset<Row> result = df.withColumn("new_column", df.col("column_name").cast(DataTypes.DoubleType));
   ```

4. **NVL():**
   ```java
   // Oracle/PLSQL
   // NVL(column_name, default_value);
   
   // Spark SQL on Java
   Dataset<Row> result = df.withColumn("new_column", functions.coalesce(df.col("column_name"), default_value));
   ```

5. **SYSDATE:**
   ```java
   // Oracle/PLSQL
   // SELECT SYSDATE FROM dual;
   
   // Spark SQL on Java
   java.util.Date sysdate = new java.util.Date();
   ```

6. **JSON_VALUE():**
   ```java
   // Oracle/PLSQL
   // JSON_VALUE(json_column, '$.key');
   
   // Spark SQL on Java
   Dataset<Row> result = df.withColumn("new_column", functions.get_json_object(df.col("json_column"), "$.key"));
   ```

7. **MONTHS_BETWEEN():**
   ```java
   // Oracle/PLSQL
   // MONTHS_BETWEEN(date1, date2);
   
   // Spark SQL on Java
   Column monthsBetween = functions.months_between(df.col("date1"), df.col("date2"));
   Dataset<Row> result = df.withColumn("months_between", monthsBetween);
   ```

8. **TO_CHAR():**
   ```java
   // Oracle/PLSQL
   // TO_CHAR(date_column, 'YYYY-MM-DD');
   
   // Spark SQL on Java
   Dataset<Row> result = df.withColumn("formatted_date", functions.date_format(df.col("date_column"), "yyyy-MM-dd"));
   ```

9. **CASE:**
   ```java
   // Oracle/PLSQL
   // CASE WHEN condition1 THEN result1 WHEN condition2 THEN result2 ELSE default_result END;
   
   // Spark SQL on Java
   Column condition1 = df.col("condition1");
   Column condition2 = df.col("condition2");
   Column result = functions.when(condition1, "result1").when(condition2, "result2").otherwise("default_result");
   Dataset<Row> transformedDF = df.withColumn("new_column", result);
   ```

10. **AVG():**
    ```java
    // Oracle/PLSQL
    // SELECT AVG(column_name) FROM table_name;
    
    // Spark SQL on Java
    Dataset<Row> result = df.agg(functions.avg("column_name"));
    ```

1. **IF-THEN-ELSE:**
   ```java
   // Oracle/PLSQL
   // IF condition THEN statements1; ELSE statements2; END IF;
   
   // Spark SQL on Java
   Column condition = df.col("condition");
   Column result = functions.when(condition, df.col("statements1")).otherwise(df.col("statements2"));
   Dataset<Row> transformedDF = df.withColumn("result_column", result);
   ```

2. **SUM():**
   ```java
   // Oracle/PLSQL
   // SELECT SUM(column_name) FROM table_name;
   
   // Spark SQL on Java
   Dataset<Row> result = df.agg(functions.sum("column_name"));
   ```

3. **TO_NUMBER():**
   ```java
   // Oracle/PLSQL
   // TO_NUMBER(column_name);
   
   // Spark SQL on Java
   Dataset<Row> result = df.withColumn("new_column", df.col("column_name").cast(DataTypes.DoubleType));
   ```

4. **NVL():**
   ```java
   // Oracle/PLSQL
   // NVL(column_name, default_value);
   
   // Spark SQL on Java
   Dataset<Row> result = df.withColumn("new_column", functions.coalesce(df.col("column_name"), default_value));
   ```

5. **SYSDATE:**
   ```java
   // Oracle/PLSQL
   // SELECT SYSDATE FROM dual;
   
   // Spark SQL on Java
   java.util.Date sysdate = new java.util.Date();
   ```

6. **JSON_VALUE():**
   ```java
   // Oracle/PLSQL
   // JSON_VALUE(json_column, '$.key');
   
   // Spark SQL on Java
   Dataset<Row> result = df.withColumn("new_column", functions.get_json_object(df.col("json_column"), "$.key"));
   ```

7. **MONTHS_BETWEEN():**
   ```java
   // Oracle/PLSQL
   // MONTHS_BETWEEN(date1, date2);
   
   // Spark SQL on Java
   Column monthsBetween = functions.months_between(df.col("date1"), df.col("date2"));
   Dataset<Row> result = df.withColumn("months_between", monthsBetween);
   ```

8. **TO_CHAR():**
   ```java
   // Oracle/PLSQL
   // TO_CHAR(date_column, 'YYYY-MM-DD');
   
   // Spark SQL on Java
   Dataset<Row> result = df.withColumn("formatted_date", functions.date_format(df.col("date_column"), "yyyy-MM-dd"));
   ```

9. **CASE:**
   ```java
   // Oracle/PLSQL
   // CASE WHEN condition1 THEN result1 WHEN condition2 THEN result2 ELSE default_result END;
   
   // Spark SQL on Java
   Column condition1 = df.col("condition1");
   Column condition2 = df.col("condition2");
   Column result = functions.when(condition1, "result1").when(condition2, "result2").otherwise("default_result");
   Dataset<Row> transformedDF = df.withColumn("new_column", result);
   ```

10. **AVG():**
    ```java
    // Oracle/PLSQL
    // SELECT AVG(column_name) FROM table_name;
    
    // Spark SQL on Java
    Dataset<Row> result = df.agg(functions.avg("column_name"));
    ```


21. **TO_DATE():**
    ```java
    // Oracle/PLSQL
    // SELECT TO_DATE('01-01-2023', 'DD-MM-YYYY') FROM dual;
    
    // Spark SQL on Java
    Dataset<Row> result = spark.sql("SELECT to_date('01-01-2023', 'dd-MM-yyyy')");
    ```

22. **NVL():**
    ```java
    // Oracle/PLSQL
    // SELECT NVL(column_name, default_value) FROM table_name;
    
    // Spark SQL on Java
    Dataset<Row> result = df.withColumn("new_column", functions.when(df.col("column_name").isNull(), "default_value").otherwise(df.col("column_name")));
    ```

23. **ROUND():**
    ```java
    // Oracle/PLSQL
    // SELECT ROUND(decimal_column, 2) FROM table_name;
    
    // Spark SQL on Java
    Dataset<Row> result = df.withColumn("rounded_column", functions.round(df.col("decimal_column"), 2));
    ```

24. **INSTR():**
    ```java
    // Oracle/PLSQL
    // SELECT INSTR(string_column, 'search_string') FROM table_name;
    
    // Spark SQL on Java
    Dataset<Row> result = df.withColumn("string_position", functions.instr(df.col("string_column"), "search_string"));
    ```

25. **CASE:**
    ```java
    // Oracle/PLSQL
    // SELECT CASE WHEN condition THEN value1 ELSE value2 END FROM table_name;
    
    // Spark SQL on Java
    Dataset<Row> result = df.withColumn("new_column", functions.when(condition, value1).otherwise(value2));
    ```

26. **XMLAGG():**
    ```java
    // Oracle/PLSQL
    // SELECT XMLAGG(XML_ELEMENT("elem", column_name)) FROM table_name;
    
    // Spark SQL on Java
    Dataset<Row> result = df.selectExpr("xml_aggregate(xml('elem', column_name)) as xml_agg");
    ```

27. **LEAD() и LAG():**
    В Spark SQL есть аналогичные аналитические функции `lead` и `lag`:
    ```java
    // Oracle/PLSQL
    // SELECT LEAD(column_name) OVER (ORDER BY column_name) FROM table_name;
    
    // Spark SQL on Java
    WindowSpec windowSpec = Window.orderBy("column_name");
    Dataset<Row> result = df.withColumn("lead_column", functions.lead(df.col("column_name")).over(windowSpec));
    ```

28. **ROWS BETWEEN:**
    Аналог для оператора `ROWS BETWEEN` из PL/SQL:
    ```java
    // Oracle/PLSQL
    // SELECT SUM(column_name) OVER (ORDER BY column_name ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM table_name;
    
    // Spark SQL on Java
    WindowSpec windowSpec = Window.orderBy("column_name").rowsBetween(-1, 1);
    Dataset<Row> result = df.withColumn("sum_column", functions.sum(df.col("column_name")).over(windowSpec));
    ```

29. **GROUP BY:**
    Пример использования `GROUP BY` в Spark SQL:
    ```java
    // Oracle/PLSQL
    // SELECT column_name, SUM(column2) FROM table_name GROUP BY column_name;
    
    // Spark SQL on Java
    Dataset<Row> result = df.groupBy("column_name").agg(functions.sum("column2"));
    ```

30. **DISTINCT:**
    Пример использования `DISTINCT` в Spark SQL:
    ```java
    // Oracle/PLSQL
    // SELECT DISTINCT column_name FROM table_name;
    
    // Spark SQL on Java
    Dataset<Row> result = df.select("column_name").distinct();
    ```


31. **TO_CHAR():**
    ```java
    // Oracle/PLSQL
    // SELECT TO_CHAR(date_column, 'DD-MON-YYYY HH:MI:SS') FROM table_name;
    
    // Spark SQL on Java
    Dataset<Row> result = df.select(functions.date_format(df.col("date_column"), "dd-MMM-yyyy HH:mm:ss"));
    ```

32. **CONTINUE:**
    В Spark SQL и Java применяются структуры управления потоком, такие как циклы и условные операторы (`if`-`else`).
    ```java
    // Oracle/PLSQL
    // CONTINUE;
    
    // Spark SQL on Java
    // Применение циклов и условных операторов для управления потоком выполнения.
    ```

33. **DBMS_OUTPUT.PUT_LINE():**
    Вместо `DBMS_OUTPUT.PUT_LINE()` из PL/SQL можно использовать вывод логов в Spark SQL и Java:
    ```java
    // Oracle/PLSQL
    // DBMS_OUTPUT.PUT_LINE('Message');
    
    // Spark SQL on Java
    // Вывод логов в Spark SQL, например:
    System.out.println("Message");
    ```

34. **ROWNUM:**
    В Spark SQL используется функция `row_number()` для реализации функциональности `ROWNUM`:
    ```java
    // Oracle/PLSQL
    // SELECT * FROM table_name WHERE ROWNUM <= 10;
    
    // Spark SQL on Java
    WindowSpec windowSpec = Window.orderBy("column_name");
    Dataset<Row> result = df.withColumn("row_num", functions.row_number().over(windowSpec)).filter(df.col("row_num").leq(10));
    ```

35. **EXECUTE IMMEDIATE:**
    В Spark SQL и Java прямой эквивалент `EXECUTE IMMEDIATE` отсутствует, так как нет необходимости компилировать или выполнить SQL-запросы на лету таким же образом, как в PL/SQL. Вместо этого используется статический SQL или функции Spark SQL.

36. **REGEXP_LIKE():**
    В Spark SQL используется функция `regexp_extract()` для работы с регулярными выражениями:
    ```java
    // Oracle/PLSQL
    // SELECT * FROM table_name WHERE REGEXP_LIKE(column_name, 'pattern');
    
    // Spark SQL on Java
    Dataset<Row> result = df.filter(df.col("column_name").rlike("pattern"));
    ```

37. **DATE_PART():**
    В Spark SQL есть функция `date_part` для извлечения части даты:
    ```java
    // Oracle/PLSQL
    // SELECT DATE_PART('day', date_column) FROM table_name;
    
    // Spark SQL on Java
    Dataset<Row> result = df.select(functions.date_part("day", df.col("date_column")));
    ```

38. **TRUNC():**
    В Spark SQL используется функция `trunc()` для обрезания даты:
    ```java
    // Oracle/PLSQL
    // SELECT TRUNC(date_column, 'MM') FROM table_name;
    
    // Spark SQL on Java
    Dataset<Row> result = df.select(functions.trunc(df.col("date_column"), "MM"));
    ```

39. **CURRENT_TIMESTAMP:**
    В Spark SQL есть эквивалент функции `CURRENT_TIMESTAMP`:
    ```java
    // Oracle/PLSQL
    // SELECT CURRENT_TIMESTAMP FROM dual;
    
    // Spark SQL on Java
    Dataset<Row> result = spark.sql("SELECT current_timestamp()");
    ```

40. **MONTHS_BETWEEN():**
    В Spark SQL используется функция `months_between()` для вычисления разницы между двумя датами в месяцах:
    ```java
    // Oracle/PLSQL
    // SELECT MONTHS_BETWEEN(date1, date2) FROM table_name;
    
    // Spark SQL on Java
    Dataset<Row> result = df.select(functions.months_between(df.col("date1"), df.col("date2")));
    ```

##

1. **LEAD():**
   В Oracle SQL:
   ```sql
   SELECT column, LEAD(column, 1) OVER (ORDER BY some_column) AS next_value FROM table_name;
   ```
   В Spark SQL на Java с использованием `lead()`:
   ```java
   import org.apache.spark.sql.expressions.Window;
   import static org.apache.spark.sql.functions.*;
   
   WindowSpec windowSpec = Window.orderBy("some_column");
   Dataset<Row> result = df.withColumn("next_value", lead(df.col("column"), 1).over(windowSpec));
   ```

2. **LAG():**
   В Oracle SQL:
   ```sql
   SELECT column, LAG(column, 1) OVER (ORDER BY some_column) AS previous_value FROM table_name;
   ```
   В Spark SQL на Java с использованием `lag()`:
   ```java
   import org.apache.spark.sql.expressions.Window;
   import static org.apache.spark.sql.functions.*;
   
   WindowSpec windowSpec = Window.orderBy("some_column");
   Dataset<Row> result = df.withColumn("previous_value", lag(df.col("column"), 1).over(windowSpec));
   ```

3. **RANK():**
   В Oracle SQL:
   ```sql
   SELECT column1, RANK() OVER (PARTITION BY column2 ORDER BY column3) AS rank_column FROM table_name;
   ```
   В Spark SQL на Java с использованием `rank()`:
   ```java
   import org.apache.spark.sql.expressions.Window;
   import static org.apache.spark.sql.functions.*;
   
   WindowSpec windowSpec = Window.partitionBy("column2").orderBy("column3");
   Dataset<Row> result = df.withColumn("rank_column", rank().over(windowSpec));
   ```

4. **DENSE_RANK():**
   В Oracle SQL:
   ```sql
   SELECT column1, DENSE_RANK() OVER (ORDER BY column2) AS dense_rank_column FROM table_name;
   ```
   В Spark SQL на Java с использованием `dense_rank()`:
   ```java
   import org.apache.spark.sql.expressions.Window;
   import static org.apache.spark.sql.functions.*;
   
   WindowSpec windowSpec = Window.orderBy("column2");
   Dataset<Row> result = df.withColumn("dense_rank_column", dense_rank().over(windowSpec));
   ```

5. **NTILE():**
   В Oracle SQL:
   ```sql
   SELECT column, NTILE(4) OVER (ORDER BY some_column) AS quartile FROM table_name;
   ```
   В Spark SQL на Java с использованием `ntile()`:
   ```java
   import org.apache.spark.sql.expressions.Window;
   import static org.apache.spark.sql.functions.*;
   
   WindowSpec windowSpec = Window.orderBy("some_column");
   Dataset<Row> result = df.withColumn("quartile", ntile(4).over(windowSpec));
   ```

6. **FIRST_VALUE():**
   В Oracle SQL:
   ```sql
   SELECT column1, FIRST_VALUE(column2) OVER (ORDER BY column3) AS first_value_column FROM table_name;
   ```
   В Spark SQL на Java с использованием `first()`:
   ```java
   import org.apache.spark.sql.expressions.Window;
   import static org.apache.spark.sql.functions.*;
   
   WindowSpec windowSpec = Window.orderBy("column3");
   Dataset<Row> result = df.withColumn("first_value_column", first(df.col("column2")).over(windowSpec));
   ```

7. **LAST_VALUE():**
   В Oracle SQL:
   ```sql
   SELECT column1, LAST_VALUE(column2 IGNORE NULLS) OVER (ORDER BY column3) AS last_value_column FROM table_name;
   ```
   В Spark SQL на Java с использованием `last()`:
   ```java
   import org.apache.spark.sql.expressions.Window;
   import static org.apache.spark.sql.functions.*;
   
   WindowSpec windowSpec = Window.orderBy("column3").rowsBetween(Window.unboundedPreceding(), Window.unboundedFollowing());
   Dataset<Row> result = df.withColumn("last_value_column", last(df.col("column2"), true).over(windowSpec));
   ```

8. **MEDIAN():**
   В Oracle SQL:
   ```sql
   SELECT column1, MEDIAN(column2) OVER (PARTITION BY column3 ORDER BY column4) AS median_column FROM table_name;
   ```
   В Spark SQL на Java с использованием `percentile_approx()` для приближенного подсчета медианы:
   ```java
   import org.apache.spark.sql.expressions.Window;
   import static org.apache.spark.sql.functions.*;
   
   WindowSpec windowSpec = Window.partitionBy("column3").orderBy("column4");
   Dataset<Row> result = df.withColumn("median_column", percentile_approx(df.col("column2"), lit(0.5)).over(windowSpec));
   ```

9. **PERCENTILE_CONT():**
   В Oracle SQL:
   ```sql
   SELECT column1, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY column2) OVER (PARTITION BY column3) AS percentile_column FROM table_name;
   ```
   В Spark SQL на Java с использованием `percentile()`:
   ```java
   import org.apache.spark.sql.expressions.Window;
   import static org.apache.spark.sql.functions.*;
   
   WindowSpec windowSpec = Window.partitionBy("column3").orderBy("column2");
   Dataset<Row> result = df.withColumn("percentile_column", expr("percentile(column2, 0.5)").over(windowSpec));
   ```

10. **PERCENTILE_DISC():**
   В Oracle SQL:
   ```sql
   SELECT column1, PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY column2) OVER (PARTITION BY column3) AS percentile_disc_column FROM table_name;
   ```
   В Spark SQL на Java с использованием `expr()`:
   ```java
   import org.apache.spark.sql.expressions.Window;
   import static org.apache.spark.sql.functions.*;
   
   WindowSpec windowSpec = Window.partitionBy("column3").orderBy("column2");
   Dataset<Row> result = df.withColumn("percentile_disc_column", expr("percentile_disc(0.5) within group (order by column2)").over(windowSpec));
   ```

11. **WIDTH_BUCKET():**
   В Oracle SQL:
   ```sql
   SELECT column1, WIDTH_BUCKET(column2, 10, 100, 1000) OVER (ORDER BY column3) AS width_bucket_column FROM table_name;
   ```
   В Spark SQL на Java можно использовать `expr()`:
   ```java
   import org.apache.spark.sql.expressions.Window;
   import static org.apache.spark.sql.functions.*;
   
   WindowSpec windowSpec = Window.orderBy("column3");
   Dataset<Row> result = df.withColumn("width_bucket_column", expr("width_bucket(column2, 10, 100, 1000)").over(windowSpec));
   ```

12. **LISTAGG():**
   В Oracle SQL:
   ```sql
   SELECT column1, LISTAGG(column2, ',') WITHIN GROUP (ORDER BY column3) AS listagg_column FROM table_name GROUP BY column1;
   ```
   В Spark SQL на Java можно использовать `collect_list()` и `concat_ws()`:
   ```java
   import org.apache.spark.sql.expressions.Window;
   import static org.apache.spark.sql.functions.*;
   
   WindowSpec windowSpec = Window.partitionBy("column1").orderBy("column3");
   Dataset<Row> result = df.withColumn("listagg_column", concat_ws(",", collect_list(df.col("column2"))).over(windowSpec));
   ```

Пример сложной хранимой процедуры на Oracle PL/SQL:

```sql
-- DDL таблицы
CREATE TABLE employees (
    employee_id NUMBER,
    first_name VARCHAR2(50),
    last_name VARCHAR2(50),
    hire_date DATE,
    salary NUMBER
);

-- Хранимая процедура
CREATE OR REPLACE PROCEDURE process_employees IS
    -- Объявление переменных
    total_salary NUMBER := 0;
    emp_count NUMBER := 0;
    avg_salary NUMBER := 0;
    hire_year NUMBER;
    
    -- Курсор для выборки данных из таблицы
    CURSOR c_employees IS
        SELECT hire_date, salary FROM employees;
    
BEGIN
    -- Цикл для обработки каждой строки из курсора
    FOR emp_rec IN c_employees LOOP
        
        -- Использование IF для фильтрации данных
        IF emp_rec.hire_date IS NOT NULL THEN
            -- Использование оконной функции для вычисления суммы зарплаты
            total_salary := total_salary + emp_rec.salary 
                OVER (ORDER BY emp_rec.hire_date);
            
            -- Использование обычной функции для получения года найма сотрудника
            hire_year := EXTRACT(YEAR FROM emp_rec.hire_date);
            
            -- Использование IF для дополнительной фильтрации данных
            IF hire_year = 2021 THEN
                -- Использование обычной функции для вычисления средней зарплаты
                avg_salary := total_salary / emp_count;
                
                -- Вывод результатов
                DBMS_OUTPUT.PUT_LINE('Total Salary: ' || total_salary);
                DBMS_OUTPUT.PUT_LINE('Average Salary: ' || avg_salary);
            END IF;
        END IF;
        
        -- Увеличение счетчика
        emp_count := emp_count + 1;
    END LOOP;
    
    -- Закрытие курсора
    CLOSE c_employees;
    
EXCEPTION
    WHEN OTHERS THEN
        -- Обработка ошибок
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/
```


Пример конвертации процедуры на Java с использованием Spark SQL Dataset:
```java

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ProcessEmployees {
    public static void main(String[] args) {
        // Создание сессии Spark
        SparkSession spark = SparkSession.builder()
                .appName("Process Employees")
                .master("local")
                .getOrCreate();
        
        // Чтение данных из таблицы employees в Dataset
        Dataset<Row> employees = spark.read()
                .format("jdbc")
                .option("url", "jdbc:oracle:thin:@localhost:1521:xe")
                .option("dbtable", "employees")
                .option("user", "username")
                .option("password", "password")
                .load();
        
        // Регистрация временной таблицы для выполнения SQL-запросов
        employees.createOrReplaceTempView("employees");
        
        // Выполнение SQL-запроса с использованием функций и оконных функций
        Dataset<Row> result = spark.sql(
                "SELECT hire_date, salary, " +
                "SUM(salary) OVER (ORDER BY hire_date) AS total_salary, " +
                "COUNT(*) OVER () AS emp_count " +
                "FROM employees " +
                "WHERE hire_date IS NOT NULL");
        
        // Фильтрация данных по году найма
        result = result.filter("EXTRACT(YEAR FROM hire_date) = 2021");
        
        // Вычисление средней зарплаты
        result = result.withColumn("avg_salary", result.col("total_salary").divide(result.col("emp_count")));
        
        // Вывод результатов
        result.show();
        
        // Закрытие сессии Spark
        spark.stop();
    }
}


//Пример конвертации процедуры на Java с использованием Spark SQL Dataset без использования метода .sql():

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class ProcessEmployees {
    public static void main(String[] args) {
        // Создание сессии Spark
        SparkSession spark = SparkSession.builder()
                .appName("Process Employees")
                .master("local")
                .getOrCreate();
        
        // Чтение данных из таблицы employees в Dataset
        Dataset<Row> employees = spark.read()
                .format("jdbc")
                .option("url", "jdbc:oracle:thin:@localhost:1521:xe")
                .option("dbtable", "employees")
                .option("user", "username")
                .option("password", "password")
                .load();
        
        // Фильтрация данных по условию
        employees = employees.filter(col("hire_date").isNotNull());
        
        // Вычисление суммы зарплаты с использованием оконной функции
        employees = employees.withColumn("total_salary", sum(col("salary")).over(orderBy(col("hire_date"))));
        
        // Вычисление количества сотрудников
        employees = employees.withColumn("emp_count", count("*").over());
        
        // Фильтрация данных по году найма
        employees = employees.filter(year(col("hire_date")).equalTo(2021));
        
        // Вычисление средней зарплаты
        employees = employees.withColumn("avg_salary", col("total_salary").divide(col("emp_count")));
        
        // Вывод результатов
        employees.show();
        
        // Закрытие сессии Spark
        spark.stop();
    }
}


//В этом примере мы использовали функции из пакета org.apache.spark.sql.functions для выполнения операций фильтрации, вычисления суммы, подсчета количества и вычисления среднего значения.
```


###
- Пример конвертации сложной процедуры на Oracle PL/SQL в Spark SQL Dataset на Java:

```java
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class ProcessData {
    public static void main(String[] args) {
        // Создание сессии Spark
        SparkSession spark = SparkSession.builder()
                .appName("Process Data")
                .master("local")
                .getOrCreate();
        
        // Чтение данных из таблицы data в Dataset
        Dataset<Row> data = spark.read()
                .format("jdbc")
                .option("url", "jdbc:oracle:thin:@localhost:1521:xe")
                .option("dbtable", "data")
                .option("user", "username")
                .option("password", "password")
                .load();
        
        // Объявление переменных
        Dataset<Row> result;
        int totalCount = 0;
        
        // Перебор строк и выполнение операций
        for (Row row : data.collectAsList()) {
            int value = row.getInt(0);
            String category = row.getString(1);
            
            // Выполнение условий и операций
            if (value > 100) {
                // Выполнение запроса с использованием оконной функции
                Dataset<Row> categoryData = data.filter(col("category").equalTo(category));
                categoryData = categoryData.withColumn("row_number", row_number().over(orderBy(col("value"))));
                
                // Вычисление среднего значения
                Dataset<Row> avgData = categoryData.filter(col("row_number").leq(5));
                double avgValue = avgData.agg(avg(col("value"))).first().getDouble(0);
                
                // Вычисление суммы
                double sumValue = categoryData.agg(sum(col("value"))).first().getDouble(0);
                
                // Обновление данных в исходной таблице
                data = data.withColumn("category_avg", when(col("category").equalTo(category), avgValue).otherwise(col("category_avg")));
                data = data.withColumn("category_sum", when(col("category").equalTo(category), sumValue).otherwise(col("category_sum")));
                
                // Увеличение счетчика обработанных строк
                totalCount++;
            }
        }
        
        // Вывод результатов
        data.show();
        System.out.println("Total Count: " + totalCount);
        
        // Закрытие сессии Spark
        spark.stop();
    }
}


В этом примере мы объявляем переменные и выполняем итерацию по строкам Dataset, применяя различные операции и функции Spark SQL для выполнения условий, вычисления среднего значения, суммы и обновления данных в исходной таблице.

```

```sql
CREATE TABLE data (
  value NUMBER,
  category VARCHAR2(50),
  category_avg NUMBER,
  category_sum NUMBER
);

```

```sql
--DDL для создания таблицы "table1":

CREATE TABLE table1 (
  id NUMBER,
  name VARCHAR2(50)
);


DDL для создания таблицы "table2":

CREATE TABLE table2 (
  id NUMBER,
  value NUMBER
);


DDL для создания таблицы "table3":

CREATE TABLE table3 (
  id NUMBER,
  category VARCHAR2(50)
);


Пример использования LEFT JOIN:

SELECT table1.name, table2.value
FROM table1
LEFT JOIN table2 ON table1.id = table2.id;


--Пример использования INNER JOIN:

SELECT table1.name, table3.category
FROM table1
INNER JOIN table3 ON table1.id = table3.id;


--В этих примерах мы объединяем таблицы "table1" и "table2" по полю "id" с использованием операции LEFT JOIN, а также таблицы "table1" и "table3" по полю "id" с использованием операции INNER JOIN.
```
```java
Пример преобразования в Dataset<Row> с использованием join без использования метода .sql() в Spark SQL на Java:

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkJoinExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkJoinExample")
                .master("local")
                .getOrCreate();

        // Создание таблицы table1
        Dataset<Row> table1 = spark.createDataFrame(
                Arrays.asList(
                        RowFactory.create(1, "John"),
                        RowFactory.create(2, "Alice"),
                        RowFactory.create(3, "Bob")
                ),
                new StructType()
                        .add("id", DataTypes.IntegerType)
                        .add("name", DataTypes.StringType)
        );

        // Создание таблицы table2
        Dataset<Row> table2 = spark.createDataFrame(
                Arrays.asList(
                        RowFactory.create(1, 100),
                        RowFactory.create(2, 200),
                        RowFactory.create(4, 400)
                ),
                new StructType()
                        .add("id", DataTypes.IntegerType)
                        .add("value", DataTypes.IntegerType)
        );

        // Создание таблицы table3
        Dataset<Row> table3 = spark.createDataFrame(
                Arrays.asList(
                        RowFactory.create(1, "Category A"),
                        RowFactory.create(3, "Category B"),
                        RowFactory.create(4, "Category C")
                ),
                new StructType()
                        .add("id", DataTypes.IntegerType)
                        .add("category", DataTypes.StringType)
        );

        // Пример использования LEFT JOIN
        Dataset<Row> leftJoinResult = table1.join(table2, table1.col("id").equalTo(table2.col("id")), "left")
                .select(table1.col("name"), table2.col("value"));

        leftJoinResult.show();

        // Пример использования INNER JOIN
        Dataset<Row> innerJoinResult = table1.join(table3, table1.col("id").equalTo(table3.col("id")), "inner")
                .select(table1.col("name"), table3.col("category"));

        innerJoinResult.show();

        spark.stop();
    }
}


В этом примере мы создаем таблицы "table1", "table2" и "table3" с помощью метода createDataFrame, а затем выполняем операции LEFT JOIN и INNER JOIN с использованием метода join и выбираем нужные столбцы с помощью метода select. Результаты объединения выводятся с помощью метода show().
```


# sql