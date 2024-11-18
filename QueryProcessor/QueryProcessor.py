from QueryOptimizer.OptimizationEngine import OptimizationEngine

class QueryProcessor:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(QueryProcessor, cls).__new__(cls)
        return cls._instance

    def execute_query(self, query):
        optimization_engine = OptimizationEngine()
        optimized_query =  optimization_engine.optimizeQuery(optimization_engine.parseQuery(query))
        print(optimized_query)

    def accept_query(self):
        # penerimaan query dari user
        query_input = ""
        print("> ", end="")
        while True:
            data = input()
            if ';' in data:
                query_input += data[:data.index(';')]
                break
            query_input += (data + " ")
        query_input = query_input.strip()

        # eksekusi query
        self.execute_query(query_input)