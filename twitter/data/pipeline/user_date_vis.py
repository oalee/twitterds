import os, pandas as pd, matplotlib.pyplot as plt, yerbamate, ipdb

env = yerbamate.Environment()

save_path = os.path.join(env["plots"], "analysis", "user_date")


# First, group by year and plot
year_path = os.path.join(save_path, "user_date_year.csv")

month_year_path = os.path.join(save_path, "user_date_year_month.csv")

pandas_df_year = pd.read_csv(year_path)
pandas_df_month_year = pd.read_csv(month_year_path)


pandas_df_year.plot(x="year", y="count", kind="bar")
plt.title("Distribution of Users Creation Date by Year")
plt.xlabel("Year")
plt.ylabel("Count")

plt.margins(x=0.45)

plt.xticks(rotation=45)

plt.subplots_adjust(bottom=0.2, left=0.1, right=0.9, top=0.9, wspace=0.3, hspace=0.3)

plt.savefig(os.path.join(save_path, "user_date_year.png"))

# ipdb.set_trace()
# drop dates after >= 2023 , 3
# after month 3, 2023, it

pandas_df_month_year = pandas_df_month_year.drop(
    pandas_df_month_year[
        (pandas_df_month_year["year"] == 2023)
        & (pandas_df_month_year["month"].isin([4, 5]))
    ].index
)

# make column year_month
pandas_df_month_year["year_month"] = (
    pandas_df_month_year["year"].astype(str)
    + "-"
    + pandas_df_month_year["month"].astype(str).str.zfill(2)
)


pandas_df_month_year.plot(x="year_month", y="count", kind="bar")
plt.title("Distribution of Users Creation Date by Year and Month (after 2022)")
plt.xlabel("Year-Month")
plt.ylabel("Count")
plt.margins(x=0.45)

plt.xticks(rotation=45)

# plt.tight_layout()  # Adjusts spacing between x-labels
plt.subplots_adjust(bottom=0.2, left=0.1, right=0.9, top=0.9, wspace=0.3, hspace=1.3)

plt.savefig(os.path.join(save_path, "user_date_month_year.png"))
