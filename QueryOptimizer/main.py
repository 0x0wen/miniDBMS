from OptimizationEngine import OptimizationEngine
from QueryTree import QueryTree
from ParsedQuery import ParsedQuery
from CustomException import CustomException
import copy

test = {
    "users": {"row": 100, "cols": ['user_id','name','age','sibling','office_id']},
    "office": {"row": 80, "cols": ['office_id', 'name', 'location']},
    "address": {"row": 80, "cols": ['address_id', 'address', 'city', 'state', 'zip']},
    "salary": {"row": 80, "cols": ['salary_id', 'user_id', 'salary', 'date', 'address_id']},
}
# Example SQL query
# query_str = "SELECT name , age FROM users, abc WHERE age > 18 ORDER BY name DESC LIMIT 10"
# query_str = "UPDATE users SET age = 20, name = 'ahmed' WHERE age > 20 OR name = 'John'"
# query_str = "UPDATE users SET age = 20 WHERE name = 'John'"
# query_str = "users AS u, abc AS a, temp as t" # Testing AS
# query_str = "ORDER BY name, age LIMIT 1"
# query_str = "DELETE FROM table WHERE age = 20 AND name = 'John' OR age = 30"
# query_str = "BEGIN TRANSACTION"
# query_str = "SELECT users.name, users.id FROM users NATURAL JOIN address JOIN office ON users.office_id = office.office_id, salary"
query_str = "SELECT users.name, users.age FROM users JOIN salary ON users.salary_id = salary.salary_id JOIN office ON users.office_id = office.office_id JOIN houses ON houses.house_id = office.office_id "
# Initialize the optimization engine
engine = OptimizationEngine()

def reverseQueryTree(tree):
    if tree.node_type == 'SELECT':
        tree = tree.children[0]
    
    if (tree.children[0].node_type != 'TJOIN' and tree.children[0].node_type != 'JOIN') and (tree.children[1].node_type != 'TJOIN' and tree.children[1].node_type != 'JOIN'):
        temp = copy.copy(tree.children[0])
        temp.node_type = 'Value2'
        temp2 = copy.copy(tree.children[1])
        temp2.node_type = 'Value1'
        tree.children[0] = temp2
        tree.children[1] = temp
        return tree
    
    if (tree.children[0].node_type != 'TJOIN' and tree.children[0].node_type != 'JOIN'):
        newTree = reverseQueryTree(tree.children[1])
        parent = copy.copy(newTree)
        while (newTree.children[0].node_type != 'Value1' or newTree.children[1].node_type != 'Value2' ):
            if(newTree.children[0].node_type == 'Value2' and newTree.children[1].node_type == 'Value1'):
                break
            if (newTree.children[0].node_type == 'Value1'):
                newTree = newTree.children[1]
            else:
                newTree = newTree.children[0]
                
        temp = copy.copy(tree.children[0])
        tree.children[0] = newTree.children[1]
        tree.children[1] = temp
        newTree.children[1] = tree
        return parent
    
    
    
    
    return tree


def rule8(tree):
    valSelect = []
    root = None
    if tree.node_type == 'SELECT':
        for i in (tree.val):
            valSelect.append(i)
        tree = tree.children[0]
        root = tree
        
    for i in (tree.val):
        valSelect.append(i)
        
    
    while ((len(tree.children) > 0 and (tree.children[0].node_type != 'JOIN' or tree.children[0].node_type != 'TJOIN')) or 
       (len(tree.children) > 1 and (tree.children[1].node_type != 'JOIN' or tree.children[1].node_type != 'TJOIN'))):
        cek1 = False
        cek2 = False
        if len(tree.children) > 0 and (tree.children[0].node_type == 'JOIN' or tree.children[0].node_type == 'TJOIN'):
            for i in tree.children[0].val:
                valSelect.append(i)
            tree = tree.children[0]
            cek1 = True

        if len(tree.children) > 1 and (tree.children[1].node_type == 'JOIN' or tree.children[1].node_type == 'TJOIN'):
            for i in tree.children[1].val:
                valSelect.append(i)
            tree = tree.children[1]
            cek2 = True
            
            
        if (cek1 == False and cek2 == False):
            break
    
    seen = set()
    uniqueList = []

    for item in valSelect:
        if item not in seen:
            uniqueList.append(item)
            seen.add(item)
    print(uniqueList)
    
    tree = root
    users_elements = [item for item in uniqueList if 'users' in item]
    
    while(True):
        cek1 = False
        cek2 = False
        
        if len(tree.children) > 0 and (tree.children[0].node_type != 'JOIN' and tree.children[0].node_type != 'TJOIN'):
            temp = QueryTree(node_type="SELECT", val=[item for item in uniqueList if tree.children[0].val[0] in item])
            temp2 = copy.copy(tree.children[0])
            tree.children[0] = temp
            temp.children.append(temp2)
            cek1 = True
        if len(tree.children) > 1 and (tree.children[1].node_type != 'JOIN' and tree.children[1].node_type != 'TJOIN'):
            temp = QueryTree(node_type="SELECT", val=[item for item in uniqueList if tree.children[1].val[0] in item])
            temp2 = copy.copy(tree.children[1])
            tree.children[1] = temp
            temp.children.append(temp2)
            cek2 = True
            
        if cek1 == True and cek2 == True:
            break
        elif cek1 == True:
            tree = tree.children[1]
        elif cek2 == True:
            tree = tree.children[0]
        else:
            break
    
    print(root)
        
        
            
            
        
    
        
    # return tree
    
    
# Parse the query
try:
    parsed_query = engine.parseQuery(query_str, test)
except CustomException as e:
    print(e)
    exit(1)
except Exception as e:
    print(f"Exception: {e}")
    exit(1)
    
print(f"{parsed_query.query_tree}")
tree = reverseQueryTree(parsed_query.query_tree)
parsed_query.query_tree.children[0] = tree
completeTree = parsed_query.query_tree
print(f"{parsed_query.query_tree}")
print("-----------------------------------------------")
rule8(parsed_query.query_tree)

# parsed_query = engine.optimizeQueryTree(parsed_query, test)

# is_valid = engine.validateParsedQuery(parsed_query.query_tree)
# if is_valid:
#     print("Query is valid and ready for optimize.")
# else:
#     print("Query contains syntax errors.")
    
# # Optimize the query
# optimized_query = engine.optimizeQuery(parsed_query)
# print(f"Optimized Query Estimated Cost: {optimized_query.estimated_cost}")