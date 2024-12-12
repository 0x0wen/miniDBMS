from StorageManager.manager.TableManager import TableManager
# Schema untuk tabel employees
employees_schema = [
    ('employee_id', 'int', 4),
    ('name', 'varchar', 50),
    ('position', 'varchar', 50),
]

# Schema untuk tabel payroll
payroll_schema = [
    ('employee_id3', 'int', 4),
    ('salary', 'float', 4),
    ('bonus', 'float', 4),
]

# Data untuk tabel employees
n_data = 5
employees_data = [
    (i, f'Employee {i}', f'Position {i}') for i in range(1, n_data + 1)
]

# Data untuk tabel payroll
payroll_data = [
    (i, 3000 + i * 100, 200 + i * 10) for i in range(1, n_data + 1)
]

# Inisialisasi TableManager
serializer = TableManager()

# Nama tabel
employees_table = "employees"
payroll_table = "payroll"

# Menulis tabel employees dan payroll
serializer.writeTable(employees_table, employees_data, employees_schema)
serializer.writeTable(payroll_table, payroll_data, payroll_schema)

# Membaca data dari tabel employees dan payroll
employees_with_schema = serializer.readTable(employees_table)
payroll_with_schema = serializer.readTable(payroll_table)


print("Data Employees:")
for row in employees_with_schema:
    print(row)

print("\nData Payroll:")
for row in payroll_with_schema:
    print(row)