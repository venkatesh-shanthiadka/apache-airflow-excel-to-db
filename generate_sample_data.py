import pandas as pd
import os

data = {
    'id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    'age': [25, 30, 35, 40, 45],
    'city': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
}

df = pd.DataFrame(data)

os.makedirs('data', exist_ok=True)
df.to_excel('data/sample_data.xlsx', index=False)
print("Sample data created at data/sample_data.xlsx")
