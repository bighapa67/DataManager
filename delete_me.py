import pandas as pd

# Create a DataFrame
df = pd.DataFrame({
    'A': range(1, 6),
    'B': range(10, 15)
})

# Suppose we want to change the values in column 'B' where column 'A' is greater than 3
# The recommended way to do this to avoid SettingWithCopyWarning is:
df.loc[df['A'] > 3, 'B'] = 99

print(df)