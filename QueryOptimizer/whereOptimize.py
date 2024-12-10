from QueryOptimizer.ParsedQuery import ParsedQuery
from QueryOptimizer.QueryTree import QueryTree


def getWhereNode(tree: QueryTree):  # type: ignore
    listChildren = []
    for child in tree.children:
        if child.node_type == "WHERE" and "." not in child.val[2]:
            if child.children:
                listChildren += getWhereNode(child) + [
                    {"Column": child.val[0].split(".")[0], "value": child.val}
                ]
            else:
                return [{"Column": child.val[0].split(".")[0], "value": child.val}]
        else:
            if tree.children:
                listChildren += getWhereNode(child)
    return listChildren


def buildNewTree(tree: QueryTree, whereNode: list):
    newTree = QueryTree(tree.node_type, tree.val, tree.parent)
    if newTree.node_type == "WHERE" and not "." in newTree.val[2]:
        return buildNewTree(tree.children[0], whereNode)

    for child in tree.children:
        if child.node_type.lower().startswith("value"):
            if child.val[0].split(".")[0] in [node["Column"] for node in whereNode]:
                whereChildNode = QueryTree(
                    "WHERE",
                    [
                        node["value"]
                        for node in whereNode
                        if node["Column"] == child.val[0]
                    ],
                    newTree,
                )
                whereChildNode.children.append(child)
                newTree.children.append(whereChildNode)
        else:
            if child.node_type == "WHERE" and child.val[0].split(".")[0] in [
                node["Column"] for node in whereNode
            ]:
                newTree.children.append(buildNewTree(child, whereNode))
            else:
                newTree.children.append(buildNewTree(child, whereNode))
    return newTree


def optimizeWhere(query: ParsedQuery) -> ParsedQuery:
    tree = query.query_tree

    whereNode = getWhereNode(tree)

    newTree = buildNewTree(tree, whereNode)

    return ParsedQuery(query.query, newTree)
