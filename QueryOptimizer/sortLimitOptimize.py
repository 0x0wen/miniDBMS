from QueryOptimizer.ParsedQuery import ParsedQuery
from QueryOptimizer.QueryTree import QueryTree


def getSortNode(tree: QueryTree):
    listChildren = []
    for child in tree.children:
        if child.node_type == "SORT":
            listChildren += getSortNode(child) + [
                {"Column": child.val[0].split(".")[0], "value": child.val}
            ]
            break
        else:
            if tree.children:
                listChildren += getSortNode(child)
    return listChildren


def getLimitNode(tree: QueryTree):
    listChildren = []
    for child in tree.children:
        if child.node_type == "LIMIT":
            listChildren += getLimitNode(child) + [{"value": child.val[0]}]
            break
        else:
            if tree.children:
                listChildren += getLimitNode(child)
    return listChildren


def buildNewTree(tree: QueryTree):
    newTree = QueryTree(tree.node_type, tree.val, tree.parent)
    if tree.node_type == "SORT" or tree.node_type == "LIMIT":
        if tree.children:
            newTree = buildNewTree(tree.children[0])
        else:
            return None

    if newTree == None:
        return None

    for child in tree.children:
        temp = buildNewTree(child)
        if temp:
            newTree.children.append(temp)
    return newTree


def optimizeSortLimit(query: ParsedQuery) -> ParsedQuery:
    tree = query.query_tree

    sortNode = getSortNode(tree)
    limitNode = getLimitNode(tree)

    root = None
    if limitNode:
        root = QueryTree("LIMIT", [limitNode[0]["value"]], None)

    if sortNode:
        if root:
            root.children.append(QueryTree("SORT", [sortNode[0]["value"]], root))
        else:
            root = QueryTree("SORT", [sortNode[0]["value"]], None)

    if not root:
        return query

    newTree = buildNewTree(tree)

    if root.children:
        root.children[0].children.append(newTree)
    else:
        root.children.append(newTree)

    return ParsedQuery(query.query, root)
