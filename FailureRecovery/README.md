# IF3140 Database Systems Mega Projects - Amazing RDS with Elixir - 2024
> Query Processor Group in Amazing RDS Super Group

## 🌠 Project Overview


## 💁‍♂️ Group Member
<table>
    <tr>
        <td colspan="3", align = "center"><center>Group Name: Amazing RDS with Elixir</center></td>
    </tr>
    <tr>
        <td>No.</td>
        <td>Name</td>
        <td>Student ID</td>
    </tr>
    <tr>
        <td>1.</td>
        <td>Christian Justin Hendrawan</td>
        <td>13522135</td>
    </tr>
    <tr>
        <td>2.</td>
        <td>Ahmad Rafi Maliki</td>
        <td>13522137</td>
    </tr>
    <tr>
        <td>3.</td>
        <td>Ahmad Thoriq Saputra</td>
        <td>13522141</td>
    </tr>
        <tr>
        <td>4.</td>
        <td>Auralea Alvinia Syaikha</td>
        <td>13522148</td>
    </tr>
</table>

## 🔑 Key Features
The Failure Recovery Manager is used to ensure data consistency and durability in the event of failures. It interacts with various components of the database system, such as the Query Processor, Concurrency Control Manager, and the Storage Manager. The Query Processor interacts with the Failure Recovery to ensure that changes made by queries are logged and can be recovered in case of a failure. The Concurrency Control Manager interacts with the Failure Recovery to handle transaction conflicts and ensure data consistency. Whilst, the Storage Manager interacts with the Failure Recovery class to ensure that changes are logged and can be recovered in case of a failure. There are seven classes in Failure Recovery Manager such as follows.

## 🧑‍⚖️ Workload Distribution
To ensure efficient development and collaboration, we have strategically distributed the workload among team members, as detailed [here](./docs/workload-distribution.md).

## 🏭 Class Diagram of Failure Recovery
The class diagram of the Failure Recovery provides a comprehensive overview of its structure, showcasing the key components, their relationships, and interactions that drive the query processing workflow [here](./docs/ClassDiagram.md).

## 🗼 Program Structure
```bash
├── FailureRecovery.py
├── LogManager.py
├── Structs
│   ├── Buffer.py
│   ├── RecoverCriteria.py
│   ├── Row.py
│   ├── Table.py
│   └── __init__.py
├── _Test
│   ├── Test1.py
│   ├── Test2.py
│   ├── Test3.py
│   └── __init__.py
└── __init__.py
```

## 🔓 Requirements
1. Python Programming Language (https://www.python.org/downloads/)

## 🏃‍♂️ How to Run Unit Testing
1. Run this to navigate to route directory `cd ../`.
2. Execute this command in terminal `python -m FailureRecovery.test` 
