import pandas as pd, matplotlib.pyplot as plt, os, yerbamate, ipdb
import seaborn as sns
from bidi.algorithm import get_display
import arabic_reshaper
import matplotlib

matplotlib.font_manager.fontManager.addfont("/usr/share/fonts/TTF/XB-Zar-Regular.ttf")

matplotlib.rcParams["font.family"] = "XB Zar"
matplotlib.rcParams["axes.unicode_minus"] = False
env = yerbamate.Environment()


save_path = os.path.join(env["plots"], "analysis", "hashtag.parquet")

df = pd.read_parquet(save_path)

odf = df.copy()

number_hashtags_to_plot = 20

# columns: hashtag, count, year, month, day

# find top 20 unique hashtags, and plot the progression of their counts over time

# find top 20 unique hashtags
# Group by 'hashtag' and get the sum of 'count' for each hashtag, then sort in descending order
top_n = (
    df.groupby("hashtag")["count"]
    .sum()
    .sort_values(ascending=False)
    .head(number_hashtags_to_plot)
)

# Get the top 20 hashtags
top_n_hashtags = top_n.index.values

# Now, let's plot the progression of counts for each of these hashtags over time

# First, filter the dataframe to only include the top 20 hashtags
df = df[df["hashtag"].isin(top_n_hashtags)]

# after 2022-08-01, thhere is not date
df["date"] = pd.to_datetime(df[["year", "month", "day"]])

df = df[df["date"] > "2022-08-01"]

# Then, create a pivot table with 'year', 'month', 'day' as index, 'hashtag' as columns and 'count' as values.
pivot_df = df.pivot_table(
    index=["date"], columns="hashtag", values="count", fill_value=0
)
pivot_df.columns = [
    get_display(arabic_reshaper.reshape(col)) for col in pivot_df.columns
]

if env.action == "heatmap":
    plt.figure(figsize=(12, 8))
    sns.heatmap(pivot_df, cmap="vlag")

    plt.title(f"Heatmap of Top {number_hashtags_to_plot} Hashtags Over Time")
    plt.show()
# ipdb.set_trace()

if env.action == "stackplot":
    plt.figure(figsize=(12, 8))

    # Generate a stacked area plot
    plt.stackplot(pivot_df.index, pivot_df.T, labels=pivot_df.columns, alpha=0.8)

    plt.title(
        get_display(
            arabic_reshaper.reshape(
                f"Progression of Top {number_hashtags_to_plot} Hashtags Over Time"
            )
        )
    )
    plt.legend(loc="upper left")

    plt.show()
if env.action == "pie":
    # df.set_index('date', inplace=True)

    # # Create a list of all unique year and month pairs in the dataframe

    # # Create Year and Month columns
    # df['Year'] = df['date'].dt.year
    # df['Month'] = df['date'].dt.month
    # ipdb.set_trace()
    # Get unique year-month pairs
    df = odf.copy()
    df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    # only after 2022-08-01
    df = df[df['date'] > '2022-01-01']

    year_month_pairs = df[["year", "month"]].drop_duplicates()

    # Iterate over each unique year and month
    for _, row in year_month_pairs.iterrows():
        year, month = row["year"], row["month"]

        # Filter data for the current year and month
        data_current_month = df[(df["year"] == year) & (df["month"] == month)]

        # Get top 10 hashtags for the current month
        top_10_hashtags = (
            data_current_month.groupby("hashtag")["count"]
            .sum()
            .nlargest(number_hashtags_to_plot)
        )
        # Reshape and reverse the column names for display
        top_10_hashtags.index = [
            get_display(arabic_reshaper.reshape(col)) for col in top_10_hashtags.index
        ]

        # Create the pie chart
        fig, ax = plt.subplots(figsize=(10, 6))
        # plt.pie(top_10_hashtags, labels = top_10_hashtags.index, autopct='%1.1f%%', startangle=140)
        wedges, texts, autotexts = ax.pie(
            top_10_hashtags, autopct="%1.1f%%", pctdistance=0.85, startangle=140
        )

        # Draw circle
        centre_circle = plt.Circle((0, 0), 0.70, fc="white")
        fig = plt.gcf()
        fig.gca().add_artist(centre_circle)

        # Equal aspect ratio ensures that pie is drawn as a circle
        ax.axis("equal")

        plt.setp(autotexts, size=8, weight="bold")

        # Create a legend
        ax.legend(
            wedges,
            top_10_hashtags.index,
            title="Hashtags",
            # loc="center right",
            # bbox_to_anchor=(1, 0, 0.5, 1),  ncol=2
        )
        plt.title(
            get_display(
                arabic_reshaper.reshape(
                    f"Top {number_hashtags_to_plot} Hashtags for {month}-{year}"
                )
            )
        )

        plt.show()
    pass
ipdb.set_trace()
# Then, we can plot the dataframe
fig, ax = plt.subplots(figsize=(12, 8))

# Then, we can plot the dataframe
pivot_df.plot(kind="line", ax=ax, cmap="tab20")

# Get the lines and labels
lines, labels = ax.get_legend_handles_labels()


labels = [get_display(arabic_reshaper.reshape(label)) for label in labels]


# Create a legend with a smaller font size
ax.legend(lines, labels, loc="best", fontsize="small")

# Add a title
plt.title(f"Progression of Top {number_hashtags_to_plot} Hashtags Over Time")

# Display the plot
# plt.show()


# save plot
save_path = os.path.join(env["plots"], "analysis", "hashtag_vis")

if not os.path.exists(save_path):
    os.makedirs(save_path, exist_ok=True)

save_file = os.path.join(
    save_path, f"hashtag_progression_{number_hashtags_to_plot}.png"
)


# need to replot to save

plt.savefig(save_file)
