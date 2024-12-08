class QueryTree:
    def __init__(self, node_type: str, val: list, parent: "QueryTree" = None):
        self.node_type = node_type         # The type of the node (e.g., SELECT, WHERE, JOIN)
        self.val = val           # Array of values associated with this node
        self.children = []         # List of child QueryTree nodes
        self.parent = parent     # Parent node in the tree (default is None for root node)

    def __repr__(self):
        return self.__str__()

    def __str__(self, level: int = 0):
        """
        Creates a string representation of the entire tree structure.
        """
        # Current node representation
        ret = "  " * level + f"{level} QueryTree(node_type: {self.node_type}, values: {self.val})"
        
        # Add all children representations
        for child in self.children:
            ret += "\n" + child.__str__(level + 1)
            
        return ret