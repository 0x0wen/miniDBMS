# miniDBMS
Developed by Amazing RDS, a team of 25.


## Naming conventions
1. Class names              : PascalCase (ex: QueryProcessor)
2. Function/Method names    : camelCase (ex: processInput)
3. Variable/Property names  : snake_case (ex: query_output)
4. Magic method names       : __doubleunderscore__ (ex: __init__)
5. Constant names           : UPPERCASE with underscores (ex: TIME_LIMIT)

## Query Tree Description
1. SELECT --> node_type : "SELECT", value : list of table name (ex: ["name, "age", "salary"]).
2. FROM --> node_type : "FROM", value : only one table name (ex: ["employee"]). *if FROM more than one table (ex: (FROM a, b) or (FROM a JOIN b)) the node type will be "JOIN" or "TJOIN".
3. JOIN --> has 3 case, cross product, natural join, and join with condition (JOIN ON). Example query tree:
   ![image](https://github.com/user-attachments/assets/fe099fb0-6d91-466e-ad28-abe40cbe8af8)
   - case 'cross product' --> node_type : "JOIN", value : none, children : has 2 children, the children will be table_node or join_node
   - case 'natural join' --> node_type : "TJOIN", value : none, children : has 2 children, the children will be table_node
   - case 'join on' --> node_type : "TJOIN", value : the condition for join (ex: ["a.id", "=", "b.id", "or", "a.city", "!=", "b.city"]), children : atleast has 2 children, the children will be table_node or join_node (if has 'and' condition)
   - *table_node --> node_type : "Value1" or "Value2", value : table name (ex: ["employee"])
   - *if in case 'join on' the condition has 'and', it will be (i can't explain :) so, just look the example)
       - query_str = "SELECT name , age FROM a, b JOIN c ON b.id = c.id AND b.city != c.city, d, e"
       - ![image](https://github.com/user-attachments/assets/6f31923f-dc21-4755-9afa-e9d239146c80)
       - i hope u can understand hehe
