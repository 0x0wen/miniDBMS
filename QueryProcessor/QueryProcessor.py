from QueryOptimizer.OptimizationEngine import OptimizationEngine

class QueryProcessor:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(QueryProcessor, cls).__new__(cls)
        return cls._instance

    def execute_query(self, query):
        optimized_query = []
        for i in range (len(query)):
            optimization_engine = OptimizationEngine()
            optimized_query.append(optimization_engine.optimizeQuery(optimization_engine.parseQuery(query[i])))
            if (optimized_query[0].query_tree.node_type == "BEGIN_TRANSACTION"):
                return optimized_query
            
        return optimized_query

    def accept_query(self):
        query_input = ""
        print("> ", end="")
        while True:
            data = input()
            if ';' in data:
                query_input += data[:data.index(';')]
                break
            query_input += (data + " ")
        query_input = query_input.strip()

        return query_input

    def main_driver(self):
        query = []
        query.append(self.accept_query())
        optimized_query = self.execute_query(query)
        while (optimized_query[0].query_tree.node_type == "BEGIN_TRANSACTION"):
            query_temp = self.accept_query()
            if (query_temp.upper() == "COMMIT"):
                query.pop(0)
                optimized_query.pop(0)
                optimized_query = self.execute_query(query)
                break

            query.append(query_temp)

        for q in optimized_query:
            print(q)