# from StorageManager.objects.DataRetrieval import DataRetrieval,Condition
# from StorageManager.objects.JoinOperation import JoinOperation
# from StorageManager.objects.JoinCondition import JoinCondition
# from StorageManager.StorageManager import StorageManager
# #SELECT year FROM course WHERE year >= 2030 AND year < 2040 OR year > 2070 AND year <> 2080

# test1 = DataRetrieval(table=["course"], column=[], conditions=[])
# test2 = DataRetrieval(table=["student"], column=[], conditions=[])
# test3 = DataRetrieval(table=["user2"], column=[], conditions=[])
# # ret2 = DataRetrieval(table=["course"], column=[], conditions=[
# #     Condition(column="year", operation="=", operand=2030, connector=None),
# #     Condition(column="year", operation="<", operand=2040, connector="AND"),
# #     Condition(column="year", operation=">", operand=2070, connector="OR"),
# #     Condition(column="year", operation="<>", operand=2080, connector="AND")
# # ])


# #pakai index
# ret3 = DataRetrieval(table=["course"], column=['courseid'], conditions=[
#     Condition(column="courseid", operation=">", operand='40', connector=None),
#     Condition(column="courseid", operation="<", operand='50', connector="AND"),
# ])
# """
# [{'year': 2030}, {'year': 2031}, {'year': 2032}, {'year': 2033}, {'year': 2034},
#  {'year': 2035}, {'year': 2036}, {'year': 2037}, {'year': 2038}, {'year': 2039},
#   {'year': 2071}, {'year': 2072}, {'year': 2073}, {'year': 2074}, {'year': 2075},
#    {'year': 2076}, {'year': 2077}, {'year': 2078}, {'year': 2079}, {'year': 2081}, 
#    {'year': 2082}, {'year': 2083}, {'year': 2084}, {'year': 2085}, {'year': 2086}, 
#    {'year': 2087}, {'year': 2088}, {'year': 2089}, {'year': 2090}, {'year': 2091}, 
#    {'year': 2092}, {'year': 2093}, {'year': 2094}, {'year': 2095}, {'year': 2096}, 
#    {'year': 2097}, {'year': 2098}, {'year': 2099}]
# Amount :  38
# """

# retrieval = DataRetrieval(
#     table=["user2"],
#     column=[],
#     conditions=[]
# )

# sm = StorageManager()
# result1 = sm.readBlock(test1)
# result2 = sm.readBlock(test2)
# result3 = sm.readBlock(test3)
# print()
# print("ISI COURSE")
# for key, value in result1[0].items():
#     print(key)
# print()
# print("ISI STUDENT")
# for key, value in result2[0].items():
#     print(key)
# print()
# print("ISI USER 2")
# for key, value in result3[0].items():
#     print(key)

# print(JoinOperation(["val1", "val2"], JoinCondition("ON", [['courseid', '=', 'studentid'], ['coursename', '=', 'fullname']])))


from QueryOptimizer.QueryTree import QueryTree
from QueryOptimizer.OptimizationEngine import  OptimizationEngine
from QueryProcessor.QueryProcessor import QueryProcessor
from StorageManager.StorageManager import StorageManager

qp = QueryProcessor()
qo = OptimizationEngine()
sm = StorageManager()
statistics = sm.getStats()
# query = input("masukin query\n")

# try: 
query_tree = qo.optimizeQuery(qo.parseQuery(input("masukin query\n"), statistics), statistics).query_tree
print("qt nya ini\n", query_tree)

results = qp.query_tree_to_results(query_tree)
# qp.query_tree_to_update_operations(query_tree)
print("coba results")
print(results)


    # qp.query_tree_to_results(query_tree)

# except Exception as e:
    # print(e)