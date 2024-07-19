# Databricks notebook source
# MAGIC %md
# MAGIC ## Should be taken from either silver or gold model in a production scenario

# COMMAND ----------

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_squared_error, r2_score

# Load the dataset
file_path = '/Workspace/Users/fschnefeld@gmail.com/databrickscase/supermarket_sales - Sheet1.csv'
data = pd.read_csv(file_path)

# Convert categorical variables to numeric
le_branch = LabelEncoder()
le_city = LabelEncoder()
le_customer_type = LabelEncoder()
le_gender = LabelEncoder()
le_product_line = LabelEncoder()
le_payment = LabelEncoder()

data['Branch'] = le_branch.fit_transform(data['Branch'])
data['City'] = le_city.fit_transform(data['City'])
data['Customer type'] = le_customer_type.fit_transform(data['Customer type'])
data['Gender'] = le_gender.fit_transform(data['Gender'])
data['Product line'] = le_product_line.fit_transform(data['Product line'])
data['Payment'] = le_payment.fit_transform(data['Payment'])

# Convert Date and Time to datetime format
data['Date'] = pd.to_datetime(data['Date'])
data['Time'] = pd.to_datetime(data['Time'], format='%H:%M').dt.hour

# Define features and target variable
X = data.drop(columns=['Invoice ID', 'Total', 'Date'])
y = data['Total']

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train a linear regression model
model = LinearRegression()
model.fit(X_train, y_train)

# Predict on the test set
y_pred = model.predict(X_test)

# Evaluate the model
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

# Display the results
print("Mean Squared Error:", mse)
print("R-squared:", r2)

