import pandas as pd, matplotlib.pyplot as plt, os, yerbamate, ipdb
import seaborn as sns
from bidi.algorithm import get_display
import arabic_reshaper
import matplotlib

# matplotlib.font_manager.fontManager.addfont("/usr/share/fonts/TTF/XB-Zar-Regular.ttf")

# matplotlib.rcParams["font.family"] = "Arial"
matplotlib.rcParams["axes.unicode_minus"] = False
env = yerbamate.Environment()


save_path = os.path.join(env["plots"], "analysis", "hashtag.parquet")

df = pd.read_parquet(save_path)
total_top_hashtags = 10000

# Define how many hashtags per chart
hashtags_per_chart = 50

# Get top hashtags overall
top_hashtags = df.groupby("hashtag")["count"].sum().nlargest(total_top_hashtags)

# Calculate how many charts will be needed
num_charts = total_top_hashtags // hashtags_per_chart


save_path = os.path.join(env["plots"], "analysis", "top_hashtags")

if not os.path.exists(save_path):
    os.makedirs(save_path)


# group by hashtag and get the sum of count for each hashtag, then sort in descending order, get the top 10000, save csv
df.groupby("hashtag")["count"].sum().nlargest(20_000).sort_values(
    ascending=False
).to_csv(os.path.join(save_path, "top_hashtags.csv"))

for i in range(num_charts):
    # Get the hashtags for this chart
    chart_hashtags = top_hashtags[i * hashtags_per_chart : (i + 1) * hashtags_per_chart]

    # Reshape and reverse the column names for display
    chart_hashtags.index = [
        get_display(arabic_reshaper.reshape(col)) for col in chart_hashtags.index
    ]
    # Create the bar chart
    fig, ax = plt.subplots(figsize=(10, 10))
    chart_hashtags.plot(kind="barh", ax=ax, color="skyblue")
    fig.subplots_adjust(
        left=0.3, right=0.9, top=0.9, bottom=0.1, wspace=0.5, hspace=0.2
    )
    ax.yaxis.set_tick_params(pad=10)

    # ax.tick_params(axis='y', labelsize=8)
    # Invert the y-axis to have the highest value at the top
    ax.invert_yaxis()

    plt.title(
        f"Top {i*hashtags_per_chart+1}-{(i+1)*hashtags_per_chart} Hashtags from 2022-01-01 to 2023-05-01"
    )

    plt.xlabel("Count")
    # plt.ylabel('Hashtag')

    # Save the chart
    plt.savefig(
        os.path.join(
            save_path,
            f"top_hashtags_{i*hashtags_per_chart+1}-{(i+1)*hashtags_per_chart}.png",
        )
    )
    # plt.show()
