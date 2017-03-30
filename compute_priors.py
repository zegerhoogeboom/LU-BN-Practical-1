import pandas as pd
import datetime
import numpy as np
import time

dateparse = lambda x: pd.datetime.strptime(x, '%m/%d/%Y')
columns = ["num", "open_date", "open_time", "close_date", "close_time", "reported_date", "code", "description", "internal_code", "internal_description", "outcome", "level_of_offense", "jurisdiction", "borough", "precinct", "location", "premise", "park", "housing_development", "x_coord", "y_coord", "latitude", "longitude"]
df = pd.read_csv('data.csv', header=0, index_col=False, low_memory=False, names=columns)

dropped_columns = ['num', 'latitude', 'longitude', 'x_coord', 'y_coord']
df.drop(dropped_columns, axis=1, inplace=True)

df['open_date'] = pd.to_datetime(df['open_date'], format="%m/%d/%Y", errors="coerce")
df['day_of_week'] = df['open_date'].dt.weekday_name
df['open_time'] = pd.to_datetime(df['open_time'], format='%H:%M:%S')

delta = datetime.timedelta(hours=1)
df['UnixStamp'] = df['open_time'].apply(lambda d: time.mktime(d.timetuple()))
bins = np.arange(min(df['UnixStamp']), max(df['UnixStamp']) + delta.seconds, delta.seconds)

def bin_from_tstamp(tstamp):
    diffs = [abs(tstamp - bin) for bin in bins]
    return bins[diffs.index(min(diffs))]

grouped = df.groupby(df['UnixStamp'].map(
    lambda t: datetime.datetime.fromtimestamp(bin_from_tstamp(t))
))

time_slots = pd.DataFrame(map(lambda (x,y): {'time': x, 'count': y.size}, grouped.groups.iteritems()))
time_slots['count'] = map(lambda x: float(time_slots['count'][x]) / time_slots['count'].sum() * 100, range(time_slots['count'].size))
time_slots.to_csv("priors/time_slots.txt", index=False, columns=["time", "count"])

for column in df.columns:
    values = df[column].value_counts(normalize=True)
    values.to_csv("priors/%s.txt" % column)
