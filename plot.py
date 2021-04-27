import pandas
import matplotlib.pyplot

df = pandas.read_excel("./posts_parsed.xlsx")
df.columns = ["author", "timestamp"]
df.timestamp = pandas.to_datetime(df.timestamp)
df["date"] = df.timestamp.dt.date

# Filter by pseudo
#pseudo = "toto"
#df = df[df.author == pseudo]

print(df.head())
print(df.info())

by_date = df.groupby("date").size()
print(by_date)
by_date.plot()

matplotlib.pyplot.show()
