import datetime
import tabula
import os
import re
import pandas as pd
import urllib
from collections import defaultdict

def get_fn(typeOfSummary, date, part):
	assert part in [1,2]
	assert typeOfSummary in ["deaths", "confirmed", "hospitalizations"]
	return "{}{}-{}".format(
		"" if typeOfSummary == "confirmed" else typeOfSummary + "-",
		date.strftime("%m%d%Y"),
		part
	)

def get_fn_to_save(fn):
	return fn + ".csv"

def fn_exists(fn):
	return get_fn_to_save(fn) in os.listdir('../self_data/nyc')

def read_from_file(fn):
	fn2 = get_fn_to_save(fn)
	if '.csv' in fn:
		fn2 = fn2[:-4]
	return pd.read_csv(os.path.join('../self_data/nyc', fn2), index_col=[0,1])

def parse_df_pdf(dfs, typeOfSummary):
	df = None
	if typeOfSummary == "confirmed":
		df = dfs[0].dropna(how='all')
		if len(dfs) == 3:
			df = dfs[1].dropna(how='all')
		if len(df.columns) in [3,6]:
			if 'Total Cases' not in df.columns:
				if 'Unnamed: 2' in df.columns:
					df = df.rename(columns={'Unnamed: 2': 'Total Cases'})
				else:
					df = df.rename(columns={'Unnamed: 1': 'Total Cases'})
				if '.' not in df.columns:
					df = df.rename(columns={'Unnamed: 0': '.'})
			df = df[['.', 'Total Cases']].dropna(how='all')
		elif len(df.columns) == 2:
			df.columns = [".", "Total Cases"]
	elif typeOfSummary == "deaths":
		df = dfs[0]
		df = df.rename(columns={"Unnamed: 0": "Underlying Conditions", 
								"Unnamed: 1": "No Underlying Conditions",
								"Unnamed: 2": "Pending",
								"Unnamed: 3": "Total"
					})
	else:
		df = dfs[0]
		df = df.rename(columns={"Unnamed: 0": "hospitalized", "Unnamed: 1": "confirmed"})
	rows_to_drop = set(["Median Age (Range)", "Deaths"])
	total = "Total"
	total_val = None
	groups = set(["Age Group", "Age 50 and over", "Sex", "Borough"])
	prev_group_type = None
	tuples = []
	values = defaultdict(list)
	for i, row in df.iterrows():
		if row.values[-1] == "Total Cases":
			continue
		v = row[df.columns[0]]
		if v == total:
			total_val = row[df.columns[1]]
		elif v in groups:
			prev_group_type = v
		elif v in rows_to_drop:
			pass
		else: # we have a value
			try:
				v = v.replace("-", "").strip()
			except AttributeError:
				continue
			tuples.append((prev_group_type, v))
			for c in df.columns[1:]:
				v2 = row[c]
				if isinstance(v2, str):
					v2 = int(re.sub(r'\([0-99]+%\)', '', v2).replace("-", "").strip())
				values[c].append(v2)
	return pd.DataFrame(values, index=pd.MultiIndex.from_tuples(tuples, names=["first", "second"]))

def save_df(df, fn):
	df.to_csv(os.path.join('../self_data/nyc', get_fn_to_save(fn)))

def get_pdf_from_nyc_gov(fn):
	try:
		return tabula.read_pdf("https://www1.nyc.gov/assets/doh/downloads/pdf/imm/covid-19-daily-data-summary-" + fn + ".pdf")
	except urllib.error.HTTPError:
		return []

def get_data_for_data(typeOfSummary, date, part):
	fn = get_fn(typeOfSummary, date, part)
	if fn_exists(fn):
		return read_from_file(fn)
	dfs = get_pdf_from_nyc_gov(fn)
	if len(dfs) == 0:
		return pd.DataFrame()
	df = parse_df_pdf(dfs, typeOfSummary)
	save_df(df, fn)
	return df

def get_data_historical():
	cur_date = datetime.datetime(year=2020,month=3,day=17)
	for part in [1,2]:
		while cur_date.date() <= datetime.datetime.now().date():
			print("doing {}".format(get_fn("confirmed", cur_date, part)))
			_ = get_data_for_data("confirmed", cur_date, part)
			cur_date += datetime.timedelta(days=1)
	cur_date = datetime.datetime(year=2020,month=3,day=22)
	for part in [1,2]:
		while cur_date.date() <= datetime.datetime.now().date():
			print("doing {}".format(get_fn("deaths", cur_date, part)))
			_ = get_data_for_data("deaths", cur_date, part)
			cur_date += datetime.timedelta(days=1)
	cur_date = datetime.datetime(year=2020,month=3,day=24)
	for part in [1,2]:
		while cur_date.date() <= datetime.datetime.now().date():
			print("doing {}".format(get_fn("hospitalizations", cur_date, part)))
			_ = get_data_for_data("hospitalizations", cur_date, part)
			cur_date += datetime.timedelta(days=1)

def get_all_data(typeOfSummary):
	files = os.listdir('../self_data/nyc')
	if typeOfSummary in ["deaths", "hospitalizations"]:
		files = filter(lambda x: typeOfSummary in x, files)
	else:
		files = filter(lambda x: "deaths" not in x and "hospitalizations" not in x, files)
	dfs = []
	for file in files:
		df = read_from_file(file)
		if typeOfSummary in ["deaths", "hospitalizations"]:
			file = file.replace(typeOfSummary + "-", "")
		datestr, part = file[:-4].split("-")
		datetm = datetime.datetime.strptime(datestr, "%m%d%Y")
		datetm = datetm.replace(hour=10 if part == 1 else 17)
		values = df.values
		tuples = df.index.values
		new_tuples = []
		for tupleX in tuples:
			new_tuples.append((datetm, tupleX[0], tupleX[1]))
		df = pd.DataFrame(values, index=pd.MultiIndex.from_tuples(new_tuples, names=["dt", "first", "second"]), columns=df.columns)
		if '..1' in df.columns:
			df = df.drop('..1', 1)
		dfs.append(df)
	return pd.concat(dfs)

def save_all_data_to_file(typeOfSummary):
	df = get_all_data(typeOfSummary)
	df.to_csv('../self_data/nyc_{}.csv'.format(typeOfSummary))

def main():
	get_data_historical()
	save_all_data_to_file("confirmed")
	save_all_data_to_file("hospitalizations")
	save_all_data_to_file("deaths")

if __name__ == '__main__':
	main()

