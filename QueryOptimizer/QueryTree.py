class QueryTree:
    def __init__(self, type: str, val: str, parent: "QueryTree" = None):
        self.type = type         # The type of the node (e.g., SELECT, WHERE, JOIN)
        self.val = val           # The value associated with this node (e.g., column name, condition)
        self.children = []       # List of child QueryTree nodes
        self.parent = parent     # Parent node in the tree (default is None for root node)

    def add_child(self, child: "QueryTree"):
        """Add a child node to the current QueryTree node."""
        self.children.append(child)
        child.parent = self      # Set the parent of the child to the current node

    def __repr__(self):
        """String representation for debugging."""
        return f"QueryTree(type={self.type}, val={self.val}, children={len(self.children)})"

