# miniDBMS - Database System Mega Project - Amazing RDS - 2024
> mini Database Management System Development

## 🏋️‍♀️ Group Member
Developed by Amazing RDS, a team of 21 member can be access [here](./docs/member.md) for more information.


## Naming conventions

1. Class names : PascalCase (ex: QueryProcessor)
2. Function/Method names : camelCase (ex: processInput)
3. Variable/Property names : snake_case (ex: query_output)
4. Magic method names : **doubleunderscore** (ex: **init**)
5. Constant names : UPPERCASE with underscores (ex: TIME_LIMIT)


## 📑 Pre-requisite

1. Windows 7 / Ubuntu / MacOS ( Not recomended to use any os from your IF2230 OS project ( It may not work ) ) or higher
2. [Python](https://www.python.org/) (version 3.6 or higher)

##  🏃‍♂ How to run

1. Clone this repository
2. Run `python3 server.py`
3. Run `python3 client.py`
4. This Program support multi-client so you can execute this `python3 client.py` multiple time.
5. Input your query in the client terminal
6. The result will be shown in the client terminal.

## Query Tree Description

1. SELECT --> node_type : "SELECT", value : list of attribute name (ex: ["name, "age", "salary"]).
2. FROM --> node_type : "FROM", value : only one table name (ex: ["employee"]). \*if FROM more than one table (ex: (FROM a, b) or (FROM a JOIN b)) the node type will be "JOIN" or "TJOIN".
3. JOIN --> has 3 case, cross product, natural join, and join with condition (JOIN ON). Example query tree:
   ![image](https://github.com/user-attachments/assets/fe099fb0-6d91-466e-ad28-abe40cbe8af8)
   - case 'cross product' --> node_type : "JOIN", value : none, children : has 2 children, the children will be table_node or join_node
   - case 'natural join' --> node_type : "TJOIN", value : none, children : has 2 children, the children will be table_node
   - case 'join on' --> node_type : "TJOIN", value : the condition for join (ex: ["a.id", "=", "b.id", "or", "a.city", "!=", "b.city"]), children : atleast has 2 children, the children will be table_node or join_node (if has 'and' condition)
   - \*table_node --> node_type : "Value1" or "Value2", value : table name (ex: ["employee"])
   - \*if in case 'join on' the condition has 'and', it will be (i can't explain :) so, just look the example)
     - query_str = "SELECT name , age FROM a, b JOIN c ON b.id = c.id AND b.city != c.city, d, e"
     - ![image](https://github.com/user-attachments/assets/6f31923f-dc21-4755-9afa-e9d239146c80)
4. WHERE --> node_type : "WHERE", value : the condition for where (ex: ["age", ">", "20", "and", "salary", "<", "1000"]), children : has 2 children, the children will be table_node or join_node
5. ORDER BY --> node_type : "ORDER BY", value : the attribute name for order by (ex: ["name", "asc"]), children : has 1 children, the children will be table_node or join_node
6. LIMIT --> node_type : "LIMIT", value : the limit number (ex: ["10"]), children : has 1 children, the children will be table_node or join_node

## ️🔑 Key Features
### Query Processor
The Query Processor is a core component of a DBMS that interprets, optimizes, and executes user queries efficiently, transforming them into actionable operations. [Learn more about Query Processor](./QueryProcessor/README.md).

### Failure Recovery Manager
The Failure Recovery Manager is used to ensure data consistency and durability in the event of failures. It interacts with various components of the database system, such as the Query Processor, Concurrency Control Manager, and the Storage Manager. The Query Processor interacts with the Failure Recovery to ensure that changes made by queries are logged and can be recovered in case of a failure. The Concurrency Control Manager interacts with the Failure Recovery to handle transaction conflicts and ensure data consistency. Whilst, the Storage Manager interacts with the Failure Recovery class to ensure that changes are logged and can be recovered in case of a failure.

