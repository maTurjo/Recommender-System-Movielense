import re
from os import system,name
import warnings
import pandas as pd
import time
from datetime import timedelta
from imdb import Cinemagoer
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)


dataset_Diretory="./Datasets/ml-100k"
genre_df = pd.read_csv(f'{dataset_Diretory}/u.genre', sep='|', encoding='latin-1')
genre_columns = ["unknown"] + list(genre_df[genre_df.columns[0]].values)
movie_columns = ['movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url']

# Cleans title of movie year titan(2003)->titan
def clean_title(title):
    return re.sub("[\(\[].*?[\)\]]","",title)

# Step 3: get DataFrame
movies_df = pd.read_csv(f'{dataset_Diretory}/u.item', sep='|', names=movie_columns+genre_columns,
                     encoding='latin-1')

# If error happens we continue from the saved dataset uncomment the next line
# df =pd.read_csv(f'movies_with_credits.csv', sep='|', encoding='latin-1')

df=movies_df.copy()


# Step 4: IMDb instance
ia = Cinemagoer()

start_time=time.time()

# Step 5: Update DataFrame with directors and cast info
for idx, row in df.iterrows():
    try:
        if idx < 0:
            continue
        # Clear Screen code START
        if name == 'nt':
            _ = system('cls')
        else:
            _ = system('clear')
        # Clear Screen code END
        current_time=time.time()
        elapsed_time=current_time-start_time
        current_item=idx+1
        total_item=len(df)
        item_remaining=total_item-idx+1
        time_remaining=(elapsed_time/current_item)*item_remaining
        formatted_time_remaining = str(timedelta(seconds=int(time_remaining)))
        print(f"{(idx+1)} ID")
        print(f"{round((idx+1)*100/total_item,2)} % Complete")
        print(f"Time Remaining {formatted_time_remaining}")
        title =  clean_title(row['title'])
        print(f"processing {title}")
        if title=="":
            continue
        results = ia.search_movie(title)
        if not results:
            print(f"No results for: {title}")
            continue

        movie = results[0]
        ia.update(movie)

        # Get director names
        directors = [str(d) for d in movie.get('directors', [])]

        # Get top 5 cast members
        cast = [str(a) for a in movie.get('cast', [])[:5]]

        # Combine names
        people = directors + cast
        for person in people:
            if person not in df.columns:
                df.loc[:, person] = 0   # Create the column with default 0
            df.loc[idx, person] = 1  # Mark presence for this row
        
        # Save every 10 iterations
        csv_path = 'movies_with_credits.csv'
        if (idx + 1) % 10 == 0 or (idx + 1) == len(df):
            print(f"Saving progress at iteration {idx + 1}...")
            df.to_csv(csv_path, index=False)
    except Exception as e:
        print(f"{e} - Eroor happened with ID : - {idx}")
        continue 