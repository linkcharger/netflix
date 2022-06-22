from time import sleep
from pytrends.request import TrendReq #as TrendReqOriginal
import numpy as np
import pandas as pd
import datetime
from tqdm import trange
import os
import re

# This should point to where the data is, which should also be the place where you save everything, write it from root:
# like for me is C:/Users/anato/Documents/netflix/data/
root_to_directory = ''
name_of_series_file = 'netflix_series.csv'

if root_to_directory == '':
  print("You have not specified a root to directory, make sure to do so")

if name_of_series_file == 'netflix_series_xxx.csv':
  print("You have to change the name of the netflix series file to whatever it is on your machine")

# Switch to the directory
# os.chdir(root_to_directory)

# Check whether data collection is starting from scratch, or if we're appending some data that was already collected
def set_up(name_of_series_file):

  # Take in the data on series
  ser = pd.read_csv(name_of_series_file, sep = ';').astype({'Season': 'string'})

  # If there's prior data, remove the series/seasons we do not need to scrap anymore
  if os.path.exists("google_trend_data.csv"):
    print("data detected: the code will complete the database rather than starting from scratch")
    # Load it
    existing = pd.read_csv("google_trend_data.csv")
    # Remove the series that are already taken care of
    to_remove = existing[['TvSeries', 'Season']].drop_duplicates().astype({'Season': 'string'})
    ser = ser.merge(to_remove, how='left', on = ['TvSeries', 'Season'], indicator = True)
    ser = ser.loc[ser['_merge'] == 'left_only']
    ser = ser.drop('_merge', axis = 1)
  else:
    existing = pd.DataFrame()

  # Create indexes we will use for the loop
  ser['Release'] = pd.to_datetime(ser.Release)
  series = ser['mead'].unique()
  series_names = ser["TvSeries"].unique()
  grouped = ser.groupby('mead')

  return ser, series, series_names, grouped, existing


# read the input data
#regions
regions = pd.read_csv('dma_codes.txt', header=None)
regions.columns=['name', 'abrev', 'code']
regions.drop('code', axis=1, inplace=True)
regions['geoName'] = regions['name'] + [' '] + regions['abrev']
regions.drop(['name', 'abrev'], axis=1, inplace=True)
regions.loc[[17], 'geoName'] = 'Billings, MT' #Billings MT was missing and is added here
regions.loc[[197], 'geoName'] = 'Washington DC (Hagerstown MD)' #were empty so corrected to google trends strg
regions.loc[[189], 'geoName'] = 'Tri-Cities TN-VA' #were empty so corrected to google trends strg

# Set up the series we want to scrap
ser, series, series_names, grouped, res = set_up(name_of_series_file)

# It will be useful for the loop
day_delta = datetime.timedelta(days=7)

# Open the connexion
pytrends = TrendReq(
    hl='en-US',
    timeout=(2, 4),        # (connect to server timeout, get response from server timeout)
    #proxies=proxyList, 
    retries=50, 
    backoff_factor=0.5,
    )

# Just a vague idea of how long it'll take
nbr_iterations = len(ser)*4*32/3600
print("The code should take around {:.2f} hours to run if no errors are encountered".format(nbr_iterations))
print("The following bar describes the number of series that remains")


#iterate over series and add the output to DF with results
for idx_series in trange(len(series)):
  # Get the series we'll be working on in this loop, also subselect the data
  serial = series[idx_series]
  natural_name = series_names[idx_series]
  dat = grouped.get_group(serial).reset_index()

  # Data conformity, check that the meads actually start with a /
  meads = [dat[x][0] for x in dat.columns if "mead" in x]
  meads = [x if (pd.isna(x) or x[0] == "/") else "/" + x for x in meads]

  # If a 2F is there in the mead (as for Narcos's place in my side of the data) we need to remove it and hope that it works
  meads = [re.sub("2F", "", x) if not pd.isna(x) else x for x in meads]

  # There will be series where mead is NAN, we won´t be treating those, these are the indexes of the non-nan
  idx_non_nan = list(np.where(list(~pd.isna(meads)))[0])

  # Get the meads we'll search all at once
  meads = [meads[x] for x in idx_non_nan]
  
  # Loop over the release dates if there are multiple seasons.
  for season in range(len(dat.Release)):

    # Unpack object
    rel = dat.Release[season]
    season_tmp = dat.Season[season]

    # Create the week indexer, this could be built into the next loop to make things shorter
    weeks = []
    for i in range(-16, 16):
        weeks.append((rel + i*day_delta).date())
  
    # Instantiate an empty dataframe with just region name, we'll iteratively append values
    showDF = pd.DataFrame(regions)

    # Loop over the weeks
    for week in weeks:

      # Wrap everything 
      finished = False
      while not finished:
        try:
          # Prepare the pipeline mead by mead
          pytrends.build_payload(
              kw_list = meads, 
              cat=0, 
              timeframe=f'{week} {week+day_delta}',
              geo='US', 
              gprop='',
              )
          
          # Actually download the data
          oneweek=pytrends.interest_by_region(
              resolution='DMA', 
              inc_low_vol=True, 
              inc_geo_code=False)
          
          oneweek = oneweek.reset_index()
          
          finished = True

        except Exception as e:
          if (str(e) == 'The request failed: Google returned a response with code 400.'):

            # It's an error that must be solved by hand, save progress but stop the code
            res.to_csv('google_trend_data.csv', index = False)

            # Do a printout
            print("Error in the way the mead is written, correct by hand, here they are written")
            print(meads)
            raise SystemExit(0)
  
          # If there is an error, wait it out and try again
          sleep(240)
          finished = False
      
        # Sleep a bit to make sure google does not block us
        sleep(3)
      

      # Take care of column names, take into account that some meads are nans.
      name_cols = ['series--{0}--{1}'.format(week, season_tmp), 'place--{0}--{1}'.format(week, season_tmp), \
                    'actor1--{0}--{1}'.format(week, season_tmp), 'actor2--{0}--{1}'.format(week, season_tmp), 'actor3--{0}--{1}'.format(week, season_tmp)]
      name_cols = [name_cols[x] for x in idx_non_nan]

      # Add the geoName first
      full_name_cols = ['geoName']
      full_name_cols.extend(name_cols)

      # Rename the columns and add it to the full dataset
      oneweek.columns = full_name_cols
      showDF = showDF.merge(oneweek, left_on = 'geoName', right_on = 'geoName')

    # Join to the overall dataset
    showDF['TvSeries'] = natural_name
    # Remodel the output
    showDF = pd.melt(showDF, id_vars=['geoName', 'TvSeries'])
    showDF[['search_term', 'week', 'Season']] = showDF.variable.str.split('--', expand=True)
    showDF = showDF.drop('variable', axis = 1)

    # Add to main dataset
    res = pd.concat([res, showDF])

# Tranform it to format and save
res.to_csv('google_trend_data.csv', index = False)
