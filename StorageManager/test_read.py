from StorageManager.objects.DataRetrieval import DataRetrieval,Condition
from StorageManager.StorageManager import StorageManager
#SELECT year FROM course WHERE year >= 2030 AND year < 2040 OR year > 2070 AND year <> 2080

ret2 = DataRetrieval(table=["course"], column=["year"], conditions=[
    Condition(column="year", operation="=", operand=2030, connector=None),
    Condition(column="year", operation="<", operand=2040, connector="AND"),
    Condition(column="year", operation=">", operand=2070, connector="OR"),
    Condition(column="year", operation="<>", operand=2080, connector="AND")
])


#pakai index
ret3 = DataRetrieval(table=["course"], column=['courseid'], conditions=[
    Condition(column="courseid", operation=">", operand='40', connector=None),
    Condition(column="courseid", operation="<", operand='50', connector="AND"),
])

ret4 = DataRetrieval(table=["course"], column=[], conditions=[
    Condition(column="courseid", operation=">", operand='40', connector=None),
    Condition(column="courseid", operation="<", operand='50', connector="AND"),
])
"""
[{'year': 2030}, {'year': 2031}, {'year': 2032}, {'year': 2033}, {'year': 2034},
 {'year': 2035}, {'year': 2036}, {'year': 2037}, {'year': 2038}, {'year': 2039},
  {'year': 2071}, {'year': 2072}, {'year': 2073}, {'year': 2074}, {'year': 2075},
   {'year': 2076}, {'year': 2077}, {'year': 2078}, {'year': 2079}, {'year': 2081}, 
   {'year': 2082}, {'year': 2083}, {'year': 2084}, {'year': 2085}, {'year': 2086}, 
   {'year': 2087}, {'year': 2088}, {'year': 2089}, {'year': 2090}, {'year': 2091}, 
   {'year': 2092}, {'year': 2093}, {'year': 2094}, {'year': 2095}, {'year': 2096}, 
   {'year': 2097}, {'year': 2098}, {'year': 2099}]
Amount :  38
"""
cond1 = Condition(column="studentid", operation="=", operand='31', connector=None)

retrieval = DataRetrieval(
    table=["student"],
    column=['studentid'],
    conditions=[]
)
sm = StorageManager()

testdel = DataRetrieval(
    table=['course'],
    column=[],
    conditions=[
        # Condition(column='courseid',operation='=',operand='30',connector=None)
    ]
)

print("---Second read with ret4-----")
sm.readBlock(testdel)

