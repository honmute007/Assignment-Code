from error import rmse_error, r2_error, mae_error, mape_error
import BaseTable
from OperationFunction import error_by_eq, row_operation,finding_4_ideal_function
from sqlalchemy import create_engine
import pandas as pd
from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource

# Creating the local SQLite database
engine = create_engine('sqlite:///csv_database11.db')

# Database table row headings
test_table_headings = ["X", "Y"]
train_table_headings = ['X'] + [f'Y{y}(training func)' for y in range(1, 5)]
ideal_table_headings = ['X'] + [f'Y{y}(training func)' for y in range(1, 51)]

# Instances of table for database
train_data = BaseTable.TrainingDataTable(train_table_headings, "Table 1: The training data's database table:")
ideal_functions = BaseTable.IdealFunctionsTable(ideal_table_headings, "Table 2: The ideal functions’ database table:")
test_data = BaseTable.TestDataTable(test_table_headings, "The database table of the test-data")

# Creating the database in SQLite
train_data.import_csv_to_database(engine, 'train.csv')
ideal_functions.import_csv_to_database(engine, 'ideal.csv')
test_data.import_csv_to_database(engine, 'test.csv')

# Getting the data from SqLite data base into Pandas for operations
df_train_data = train_data.get_table_as_dataframe(engine)
df_ideal_functions = ideal_functions.get_table_as_dataframe(engine)
df_test_data = test_data.get_table_as_dataframe(engine)

# Data visualisation with Bokeh lib.
p1 = figure(title="Test Data", x_axis_label='X', y_axis_label='Y')
p1.scatter('X', 'Y', source=ColumnDataSource(df_test_data), size=6, color='red', alpha=0.8)
show(p1)
colors = ["red", "blue", "pink", "yellow"]
p2 = figure(title="Training data", x_axis_label='X', y_axis_label='Y')
for a in range(0, 4):
    p2.line('X', F'Y{a}(training func)', source=ColumnDataSource(df_train_data), color=f"{colors[a]}", alpha=0.8)
show(p2)

# finding the Ideal 4 equations from ideal equations which gives Least-Square
ideal_fun4 = finding_4_ideal_function(train_table_headings, ideal_table_headings, df_train_data, df_ideal_functions)
# finding Creating the DataFrame with chosen 4 ideal functions with test data
mergeTet = pd.merge(df_test_data, df_ideal_functions, on='X', how='left')
finalTest = mergeTet[['X', 'Y'] + ideal_fun4]
finalTest.fillna(0, inplace=True)
error_test = finalTest.copy()
# Cleaning the data and visualisation
# value of all the ideal equation choosen has zero value (from the figure) hence will replace x-value fill the blanc with "0"
p3 = figure(title="Choosen eq", x_axis_label='X', y_axis_label='Y')
b = 0
for a in ideal_fun4:
    p3.scatter('X', F'{a}', source=ColumnDataSource(finalTest), color=f"{colors[b]}", alpha=0.8)
    b = b + 1
show(p3)

# finding no of element in each idea function which are in range of Sq.root(2)
no_of_elements = []
for ideal_fun in ideal_fun4:
    finalTest[f"{ideal_fun} distance"] = finalTest["Y"] - finalTest[ideal_fun]
    finalTest[f"{ideal_fun} limit"] = finalTest["Y"] - finalTest[ideal_fun]
    finalTest[f"{ideal_fun} limit"] = finalTest[f"{ideal_fun} limit"].abs()
    finalTest[f"{ideal_fun} distance"] = finalTest[f"{ideal_fun} distance"].abs()
    finalTest[f"{ideal_fun} limit"] = finalTest.apply(lambda row: row_operation(row, ideal_fun), axis=1)
    no_of_elements.append(finalTest[f"{ideal_fun} limit"].sum())

ideal_fun_with_deviation = {ideal_fun4[i]: no_of_elements[i] for i in range(len(no_of_elements))}

LeastDeviation = min(zip(ideal_fun_with_deviation.keys(), ideal_fun_with_deviation.values()))[0]

# in Our example we have two functins Y41 and Y11 where N = 26 points where the distance between actual value and
# predicted values is less than sq.root (2)

r2_error = r2_error(error_test, ideal_fun4)
mae_error = mae_error(error_test, ideal_fun4)
mape_error = mape_error(error_test, ideal_fun4)
RMSE_error = rmse_error(error_test, ideal_fun4)

# creating The database table of the test-data, with mapping and y-deviation
df = finalTest[["X", "Y", f'{LeastDeviation}', f"{LeastDeviation} distance"]]
df = df.rename(columns={f'{LeastDeviation}': f'Delta Y ({LeastDeviation})',
                        f"{LeastDeviation} distance": f"{LeastDeviation} deviation"})

test_table_with_deviation = df.columns.tolist()
table_name4 = "Table 3: The database table of the test-data, with mapping and y-deviation:"

test_data_with_deviation = BaseTable.TestDataTableWithDeviation(df, test_table_with_deviation, table_name4)
test_data_with_deviation.df_to_sqlite(engine)

df = df.sort_values(by='X')
p = figure(title=f"Actual and predicted values of {LeastDeviation}", x_axis_label='x-value', y_axis_label='Y-value')

p.scatter(df['X'], df['Y'], size=10)
p.line(df['X'], df['Y'], line_width=2, line_color="blue", legend_label="Actual value of Y")
p.line(df['X'], df[f'Delta Y ({LeastDeviation})'], line_width=2, line_color="red", legend_label="predicted value of Y")
p.scatter(df['X'], df[f'Delta Y ({LeastDeviation})'], size=10, fill_color="red")

show(p)
