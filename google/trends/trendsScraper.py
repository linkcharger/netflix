#%%
import datetime
import os
import re
from random import random
from time import sleep

import numpy as np
import pandas as pd
from pytrends.request import TrendReq
from retrying import retry
from tqdm import tqdm


def waitFor(min, max):
	sleep(min + random() * (max-min))

def handleException(e):
	global resultDF, cleanIDs
	
	badRequest = (str(e) == 'The request failed: Google returned a response with code 400.')
	if badRequest:

		# it's an error that must be solved by hand, save progress but stop the code
		print('Error in the way the mID is written: correct by hand')
		print(cleanIDs)

		resultDF.to_csv('google_trend_data.csv', index=False)


    # retry if the following is true
	return not badRequest

@retry(wait_exponential_multiplier=500, wait_exponential_max=25*60*60*1_000, retry_on_exception=handleException)
def retrieveData(dataRetriever:TrendReq, cleanIDs:list, week, day_delta, geo:str, resolution:str):
	'''
	Retrieves data from google trends with specified arguments, 
	
	- upon error, checks whether exception is Error 400 (Bad Request)
		- if so, save current results and print the cause of the bad request
		- if different error, retry
	- waits 0.5 * 2^x seconds between each retry, up to 25h
	- once it reaches 25h wait time, it will wait 25h for all future exceptions and keep retrying forever
	'''
    
	# prepare the pipeline mID by mID
	dataRetriever.build_payload(
		kw_list=cleanIDs, 
		cat=0, 
		timeframe=f'{week} {week+day_delta}',
		geo=geo, 
		gprop='',
	)
					
	# download the data
	oneweek = dataRetriever.interest_by_region(
		resolution=resolution, 
		inc_low_vol=True, 
		inc_geo_code=False, 
		)

	return oneweek.reset_index()



#%%####################################################################################################################################
################################################### reading input data ################################################################
#######################################################################################################################################

####################### regions #######################
# regions = pd.read_csv('dma_codes.txt', header=None)
# regions.columns = ['name', 'abrev', 'code']
# regions = regions.drop('code', axis=1)
# regions['geoName'] = regions['name'] + [' '] + regions['abrev']
# regions = regions.drop(['name', 'abrev'], axis=1)
# regions.loc[[17], 'geoName'] = 'Billings, MT' 							# was missing
# regions.loc[[197], 'geoName'] = 'Washington DC (Hagerstown MD)' 		# empty so corrected to google trends string
# regions.loc[[189], 'geoName'] = 'Tri-Cities TN-VA' 						# empty so corrected to google trends string



####################### shows #######################
showsToScrapeDF = pd.read_excel('shows_to_scrape.xlsx', index_col=0)



####################### previous results #######################
if os.path.exists('google_trend_data.csv'):
	# if there's prior data, remove the series/seasons we do not need to scrape anymore
	print('data detected: the code will complete the database rather than starting from scratch')
	resultDF = pd.read_csv('google_trend_data.csv')

	# remove the series that are already taken care of
	to_remove = resultDF[['TvSeries', 'Season']].drop_duplicates().astype({'Season': 'string'})
	showsToScrapeDF = showsToScrapeDF.merge(to_remove, how='left', on = ['TvSeries', 'Season'], indicator = True)
	showsToScrapeDF = showsToScrapeDF.loc[showsToScrapeDF['_merge'] == 'left_only']
	showsToScrapeDF = showsToScrapeDF.drop('_merge', axis = 1)
else:
	resultDF = pd.DataFrame()



# create indexes we will use for the loop
showsToScrapeDF['Release'] = pd.to_datetime(showsToScrapeDF.Release)
mIDs = showsToScrapeDF['mID'].unique()
showNames = showsToScrapeDF['TvSeries'].unique()









#%%####################################################################################################################################
################################################# scraping and processing #############################################################
#######################################################################################################################################

day_delta = datetime.timedelta(days=7)

dataRetriever = TrendReq(
		hl='en-US',
		timeout=(2, 4),    			# (connect to server timeout, get response from server timeout)
		retries=50, 
		backoff_factor=0.5,
		#proxies=proxyList, 
		)



############################################### give a little heads up ###############################################
numberIterations = len(showsToScrapeDF)*4*32/3600
print(f'The code should take around {numberIterations:.2f} hours to run if no errors are encountered')




# iterate over series and add the output to DF with results
for mID, showName in tqdm(list(zip(mIDs, showNames))):

	# get subset of data
	currentShowDF = showsToScrapeDF[showsToScrapeDF.mID == mID]


	# check that the mIDs actually start with a /, that it doesnt have a '2F', ignore nans
	cleanIDs = [currentShowDF[column].values[0] for column in currentShowDF.columns if 'mID' in column]
	cleanIDindices = [i for i, id in enumerate(cleanIDs) if not pd.isna(id)]
	cleanIDs = [id if id[0] == '/' else ('/' + id) for id in cleanIDs if not pd.isna(id)]
	cleanIDs = [re.sub('2F', '', id) for id in cleanIDs]
		

	# loop over the release dates if there are multiple seasons.
	for i, releaseDate, seasonNumber in currentShowDF[['Release', 'Season']].itertuples():

		# loop over weeks surrounding release
		weeks = [(releaseDate + i*day_delta).date() for i in range(-16, 16)]
		for week in weeks:


			# get data from google trends
			geo='US'
			resolution='DMA'

			oneWeekRaw = retrieveData(
				dataRetriever, 
				cleanIDs, 
				week, 
				day_delta, 
				geo, 
				resolution)
					
			waitFor(3, 5)
			

			# take care of column names, take into account that some mIDs are nans.
			name_cols = [
				f'series--{week}--{seasonNumber}', 
				f'place--{week}--{seasonNumber}',
				f'actor1--{week}--{seasonNumber}', 
				f'actor2--{week}--{seasonNumber}',
				f'actor3--{week}--{seasonNumber}', 
				]
			name_cols = [name_cols[i] for i in cleanIDindices]

			# geoName first
			full_name_cols = ['geoName'] + name_cols

			# rename columns and add to full dataset
			oneWeek = oneWeekRaw.copy(deep=True)

			oneWeek.columns = full_name_cols
			try:
				tempResultsDF = tempResultsDF.merge(oneWeek, on='geoName')
			except KeyError:
				tempResultsDF = oneWeek
			break

		# join to overall dataset
		tempResultsDF['TvSeries'] = showName

		# convert to long format table
		tempResultsDF = pd.melt(tempResultsDF, id_vars=['geoName', 'TvSeries'])
		tempResultsDF[['search_term', 'week', 'Season']] = tempResultsDF.variable.str.split('--', expand=True)
		tempResultsDF = tempResultsDF.drop('variable', axis = 1)

		# append to main dataset
		resultDF = pd.concat([resultDF, tempResultsDF])

		break

# transform it to format and save
resultDF.to_csv('google_trend_data.csv', index=False)



