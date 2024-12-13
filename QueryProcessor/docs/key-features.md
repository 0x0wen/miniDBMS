# 🔑 Key Features

1. SELECT
   
    Allows users to retrieve data from one or more tables, either selecting specific columns or all columns using the * wildcard.

2. UPDATE
 
    Enables users to modify the values in records of a table using the UPDATE and SET statements. This operation supports a single condition in the WHERE clause. For example: UPDATE employee SET salary = 1.05 * salary WHERE salary > 1000;.

3. FROM

    Specifies the tables from which data will be retrieved. When multiple tables are involved, the result is the Cartesian product of those tables.

4. AS

    Allows users to assign an alias to a table for easier reference in queries. For example: SELECT * FROM student AS s, lecturer AS l WHERE s.lecturer_id = l.id;.

5. JOIN

    Supports combining tables through JOIN ON or NATURAL JOIN statements, facilitating the retrieval of related data from multiple sources.

6. WHERE

    Defines conditions that must be met for data selection, using comparison operators such as equal to (=), not equal to (<>), greater than (>), greater than or equal to (>=), less than (<), and less than or equal to (<=).

7. ORDER BY

    Allows the sorting of the selected data by a specified attribute in either ascending or descending order. Numerical attributes are ordered by their values, while string and other data types are sorted lexicographically. (Hint: The ord() function in Python can be used for string sorting.)

8. LIMIT

    Restricts the number of records returned in the result set using the LIMIT statement.

9. BEGIN TRANSACTION

    Initiates a transaction, marking the start of a sequence of operations that can be committed or rolled back.

10. COMMIT

    Finalizes and saves the changes made during a transaction.