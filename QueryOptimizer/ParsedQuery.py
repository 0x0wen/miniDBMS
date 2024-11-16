from QueryTree import QueryTree

class ParsedQuery:
    def __init__(self, query: str):
        self.query = query                # The original SQL query string
        self.query_tree = None            # QueryTree representing the parsed query structure
        self.execution_order = []         # List of operations in the execution order (logical plan)
        self.estimated_cost = 0           # Estimated cost of the query plan

    def parseQuery(self):
        """Parse the query string into a QueryTree structure."""
        tokens = self.query.split()
        root = self._parse_tokens(tokens)
        self.query_tree = root

    def _parse_tokens(self, tokens):
        """Helper method to create a QueryTree."""
        if not tokens:
            return None

        token = tokens.pop(0).upper()

        if token == "SELECT":
            root = QueryTree(type="SELECT", val="SELECT")
            root.add_child(self._parse_select(tokens))
        elif token == "FROM":
            root = QueryTree(type="FROM", val="FROM")
            root.add_child(self._parse_from(tokens))
        elif token == "WHERE":
            root = QueryTree(type="WHERE", val="WHERE")
            root.add_child(self._parse_where(tokens))
        else:
            root = QueryTree(type="UNKNOWN", val=token)

        return root

    def _parse_select(self, tokens):
        """Helper to parse SELECT clause."""
        columns = []
        while tokens and tokens[0].upper() != "FROM":
            columns.append(tokens.pop(0))
        return QueryTree(type="SELECT_COLUMNS", val=" ".join(columns))

    def _parse_from(self, tokens):
        """Helper to parse FROM clause."""
        tables = []
        while tokens and tokens[0].upper() not in ["WHERE", "JOIN"]:
            tables.append(tokens.pop(0))
        return QueryTree(type="FROM_TABLES", val=" ".join(tables))

    def _parse_where(self, tokens):
        """Helper to parse WHERE clause."""
        condition = " ".join(tokens)
        return QueryTree(type="WHERE_CONDITION", val=condition)

    def optimize_query(self, optimization_engine):
        """
        Applies optimizations to the query, modifying the execution_order and adding cost estimates.
        """
        # Reset optimization-related fields
        self.execution_order = []
        self.estimated_cost = 0

        # Apply optimizations
        optimization_engine.apply_heuristic_rules(self)
        self.estimated_cost = optimization_engine.get_cost(self)

    def __repr__(self):
        return (f"ParsedQuery(query={self.query}, estimated_cost={self.estimated_cost}, "
                f"execution_order={self.execution_order})")
