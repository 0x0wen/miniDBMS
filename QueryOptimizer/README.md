# Query Optimizer


## Query Tree's Representation
1. WHERE clause\
` WHERE name = 'Ahmed' AND age > 5 `
``` 
    Parsed to:
    QueryTree(node_type: 'WHERE', val:["name","=","'Ahmed'"]) 
        QueryTree(node_type: 'WHERE', val:["age",">","5"]) 
```
` WHERE name = 'Ahmed' OR age > 5 `
``` 
    Parsed to:
    QueryTree(node_type: 'WHERE', val:["name","=","'Ahmed'", "age",">","5"]) 
```
2. UPDATE clause\
` UPDATE employee SET name = 'Ahmad', age = 3 WHERE name = 'Ahmed' AND age > 5 `
``` 
    Parsed to:
    QueryTree(node_type: 'UPDATE', val:["employee"]) 
        QueryTree(node_type: 'SET', val:["name", "=", "'Ahmad'", ",", "age", "=", "3"]) 
            QueryTree(node_type: 'WHERE', val:["name","=","'Ahmed'"]) 
                QueryTree(node_type: 'WHERE', val:["age",">","5"]) 
```