import pandas as pd
import datetime
import numpy as np
import time
import math

dateparse = lambda x: pd.datetime.strptime(x, '%m/%d/%Y')
columns = ["num", "open_date", "open_time", "close_date", "close_time", "reported_date", "code", "description", "internal_description", "internal_description", "outcome", "level_of_offense", "jurisdiction", "borough", "precinct", "location", "premise", "park", "housing_development", "x_coord", "y_coord", "latitude", "longitude"]
df = pd.read_csv('data.csv', header=0, index_col=False, low_memory=False, names=columns)

dropped_columns = ['num', 'latitude', 'longitude', 'x_coord', 'y_coord']
df.drop(dropped_columns, axis=1, inplace=True)

df['open_date'] = pd.to_datetime(df['open_date'], format="%m/%d/%Y", errors="coerce")
df['day_of_week'] = df['open_date'].dt.weekday_name
df['month'] = df['open_date'].dt.month
df['open_time'] = pd.to_datetime(df['open_time'], format='%H:%M:%S')


df['time_slots'] = pd.cut(pd.to_numeric(df['open_time']), 4, labels=["00 - 06","06 - 12","12 - 18", "18 - 24"])

df['park'] = pd.notnull(df['park'])
df['housing_development'] = pd.notnull(df['housing_development'])

# Only use: [638.0, 101.0, 333.0, 639.0, 637.0, 338.0, 109.0, 254.0]
# df['internal_description'] = df.loc[
#     df['internal_description'].isin(
#         df['internal_description'].value_counts().axes[0][:8].values
#     )
# ]['internal_description']

# Only use: ['PETIT LARCENY', 'HARRASSMENT 2', 'ASSAULT 3 & RELATED OFFENSES', 'CRIMINAL MISCHIEF & RELATED OF',
# 'GRAND LARCENY', 'OFF. AGNST PUB ORD SENSBLTY &', 'DANGEROUS DRUGS', 'FELONY ASSAULT']
# df['description'] = df.loc[
#     df['description'].isin(
#         df['description'].value_counts().axes[0][:8].values
#     )
# ]['description']

def compute_prior_given_data(column, y):
    for column_value in column.unique():
        selected_rows = df.loc[df[column.name] == column_value][y.name].value_counts(normalize=True, dropna=False)
        for y_value in y.unique():
            if isinstance(y_value, float) and math.isnan(y_value): continue
            if selected_rows.get(y_value) is None:
                selected_rows[y_value] = 0
        normalized_column_value = ''.join(e for e in str(column_value) if e.isalnum())
        selected_rows.to_csv("priors/%s/%s_%s.txt" % (y.name, column.name, normalized_column_value))


def compute_prior_given_data2(column1, column2, y):
    for column1_value in column1.unique():
        selected_rows = df.loc[df[column1.name] == column1_value]
        for column2_value in column2.unique():
            selected_rows2 = selected_rows.loc[selected_rows[column2.name] == column2_value][y.name].value_counts(normalize=True, dropna=False)
            if not y.unique().dtype.name is 'category':
                for y_value in y.unique():
                    if isinstance(y_value, float) and math.isnan(y_value): continue
                    if selected_rows2.get(y_value) is None:
                        selected_rows2[y_value] = 0
            normalized_column1_value = ''.join(e for e in str(column1_value) if e.isalnum())
            normalized_column2_value = ''.join(e for e in str(column2_value) if e.isalnum())
            selected_rows2.to_csv("priors/%s/%s_%s_%s_%s.txt" % (y.name, column1.name, column2.name, normalized_column1_value, normalized_column2_value))


def compute_prior_given_data3(column1, column2, column3, y):
    for column1_value in column1.unique():
        selected_rows = df.loc[df[column1.name] == column1_value]
        for column2_value in column2.unique():
            selected_rows2 = selected_rows.loc[selected_rows[column2.name] == column2_value]
            for column3_value in column3.unique():
                selected_rows3 = selected_rows2.loc[selected_rows2[column3.name] == column3_value][y.name].value_counts(normalize=True, dropna=False)
                for y_value in y.unique():
                    if isinstance(y_value, float) and math.isnan(y_value): continue
                    if selected_rows3.get(y_value) is None:
                        selected_rows3[y_value] = 0
                normalized_column1_value = ''.join(e for e in str(column1_value) if e.isalnum())
                normalized_column2_value = ''.join(e for e in str(column2_value) if e.isalnum())
                normalized_column3_value = ''.join(e for e in str(column3_value) if e.isalnum())
                selected_rows3.to_csv("priors/%s/%s_%s_%s_%s_%s_%s.txt" % (y.name, column1.name, column2.name, column3.name, normalized_column1_value, normalized_column2_value, normalized_column3_value))


def compute_prior_given_data4(column1, column2, column3, column4, y):
    for column1_value in column1.unique():
        selected_rows = df.loc[df[column1.name] == column1_value]
        for column2_value in column2.unique():
            selected_rows2 = selected_rows.loc[selected_rows[column2.name] == column2_value]
            for column3_value in column3.unique():
                selected_rows3 = selected_rows2.loc[selected_rows2[column3.name] == column3_value]
                for column4_value in column4.unique():
                    selected_rows4 = selected_rows3.loc[selected_rows3[column4.name] == column4_value][y.name].value_counts(normalize=True, dropna=False)
                    for y_value in y.unique():
                        if isinstance(y_value, float) and math.isnan(y_value): continue
                        if selected_rows4.get(y_value) is None:
                            selected_rows4[y_value] = 0
                    normalized_column1_value = ''.join(e for e in str(column1_value) if e.isalnum())
                    normalized_column2_value = ''.join(e for e in str(column2_value) if e.isalnum())
                    normalized_column3_value = ''.join(e for e in str(column3_value) if e.isalnum())
                    normalized_column4_value = ''.join(e for e in str(column4_value) if e.isalnum())
                    selected_rows4.to_csv("priors/%s/%s_%s_%s_%s_%s_%s_%s_%s.txt" % (y.name, column1.name, column2.name, column3.name, column4.name, normalized_column1_value, normalized_column2_value, normalized_column3_value, normalized_column4_value))


# Single dependencies:
# compute_prior_given_data(df["description"], df["internal_description"])
# compute_prior_given_data(df["internal_description"], df["level_of_offense"])
# compute_prior_given_data(df["borough"], df["park"])
# compute_prior_given_data(df["borough"], df["housing_development"])

# Two dependencies:
compute_prior_given_data2(df["month"], df["day_of_week"], df["time_slots"])
# compute_prior_given_data2(df["internal_description"], df["level_of_offense"], df["jurisdiction"])
# compute_prior_given_data2(df["borough"], df["time_slots"], df["location"])

# Three dependencies:
# compute_prior_given_data3(df["borough"], df["park"], df["housing_development"], df["precinct"])
# compute_prior_given_data3(df["location"], df["park"], df["housing_development"], df["description"])

# Four dependencies:
# compute_prior_given_data4(df["time_slots"], df["jurisdiction"], df["internal_description"], df["level_of_offense"],
# df["outcome"])


for column in df.columns:
    values = df[column].value_counts(normalize=True)
    values.to_csv("priors/%s.txt" % column)
