from QueryOptimizer.QueryTree import QueryTree

class ParsedQuery:
    def __init__(self, query: str, query_tree: QueryTree = None):
        self.query = query          # The raw SQL query string
        self.query_tree = query_tree  # QueryTree representing the parsed structure

    def __repr__(self):
        return f"ParsedQuery(query={self.query}, query_tree={self.query_tree})"