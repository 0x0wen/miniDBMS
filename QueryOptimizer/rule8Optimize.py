import copy
from QueryOptimizer.QueryTree import QueryTree

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
        temp.node_type = 'Value2'
        temp2 = copy.copy(newTree.children[1])
        temp2.node_type = 'Value1'
        tree.children[0] = temp2
        tree.children[1] = temp
        newTree.children[1] = tree
        return parent
    
    
    
    
    return tree

def rule8(queryTree):
    valSelect = []
    root = queryTree.query_tree
    if root.node_type == 'SELECT':
        for i in (root.val):
            valSelect.append(i)
        tree = root.children[0]
        
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
    
    tree = root.children[0]
    
    while(True):
        cek1 = False
        cek2 = False
        
        if tree.children[0] not in ["JOIN", "TJOIN", "Value1","Value2"]:
            break
        
        if tree.children[1] not in ["JOIN", "TJOIN", "Value1","Value2"]:
            break

        
        if len(tree.children) > 0 and (tree.children[0].node_type != 'JOIN' and tree.children[0].node_type != 'TJOIN'):
            temp = QueryTree(node_type="SELECT", val=[item for item in uniqueList if tree.children[0].val[0]+'.' in item])
            if(len(temp.val) != 0):
                temp2 = copy.copy(tree.children[0])
                tree.children[0] = temp
                temp.children.append(temp2)
            cek1 = True
            
        if len(tree.children) > 1 and (tree.children[1].node_type != 'JOIN' and tree.children[1].node_type != 'TJOIN'):
            temp = QueryTree(node_type="SELECT", val=[item for item in uniqueList if tree.children[1].val[0]+'.' in item])
            if(len(temp.val) != 0):
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
    
    return queryTree

  