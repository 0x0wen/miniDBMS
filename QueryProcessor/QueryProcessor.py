class QueryProcessor:
    _instance = None  # Static variable to hold the single instance

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(QueryProcessor, cls).__new__(cls)
        return cls._instance

    def execute_query(self, query):
        # tinggal buat ngirim query ke query optimization
        print(query)

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