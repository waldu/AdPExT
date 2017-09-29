# -*- coding: utf-8 -*-
"""
Created on:     Sun May 21 05:09:40 2017
Author:         Waldu Woensdregt
Description:    Code uses OpenWPM package to extract HTTPS responses from
                selected set of URLs (defined in GetListOfSiteURLsToExtract)
                and then splits the resulting data into individual URL 
                parameters to allow it to be sorted and classified for use
                in a thesis masters assignment
"""
import msc_UseOpenWPM
from msc_ParamCleansing import open_db_conn
from msc_ParamCleansing import setup_db_tables
from msc_ParamCleansing import extract_parameters


def get_site_urls_to_extract():
    url_list = []
    # url_list.append('http://www.smallestwebsitetotheworld.com/')  # for testing
    # url_list.append('https://www.reddit.com/')  # for testing
    url_list.append('https://www.youtube.com/watch?v=JGwWNGJdvx8')
    url_list.append('https://www.reddit.com/')
    url_list.append('https://www.amazon.co.uk/')
    url_list.append('http://www.ebay.co.uk/itm/232254122171')
    url_list.append('http://www.ladbible.com/')
    url_list.append('https://www.yahoo.com/')
    url_list.append('https://www.theguardian.com/uk')
    url_list.append('http://diply.com/')
    url_list.append('http://imgur.com/gallery/nxrNk')
    url_list.append('http://www.dailymail.co.uk/home/index.html')
    url_list.append('https://www.twitch.tv/')
    url_list.append('http://www.imdb.com/')
    url_list.append('http://www.rightmove.co.uk/property-for-sale/property-66961808.html')
    url_list.append('http://www.telegraph.co.uk/')
    url_list.append('http://fandom.wikia.com/articles/pitch-perfect-3-teaser-trailer-drops')
    url_list.append('http://www.sportbible.com/football/transfers-barcelonas-plan-b-is-just-as-good-as-marco-verratti'
                    '-20170625')
    url_list.append('http://www.independent.co.uk/')
    url_list.append('https://www.gumtree.com/p/plumbing-central-heating/gas-fired-log-fired-central-heating-system'
                    '-cheap-/1250349004')
    url_list.append('https://wordpress.com/')
    url_list.append('http://www.msn.com/en-gb/lifestyle/family-relationships/meghan-markle-responds-to-marriage'
                    '-rumours/ar-BBCxAHD')
    return url_list


# -------------------------------------------------------------------------- #
# -                 MAIN START                                             - #
# ------------------------------------------------------------------------- -#

# init variables
myprefix = 'msc_'  # table prefix to easily keep them separate from OpenWPM
# enable/disable certain parts during testing
runDataCollection = 1  # 1 = enabled
do_extractParameters = 1  # 1 = enabled

# open database connection
conn = open_db_conn()

# connect to DB and get max crawl (to later know which crawls are new)
max_crawl_id = conn.execute('SELECT  MAX(crawl_id) FROM crawl').fetchone()[0]
print 'Max CrawlID before new crawl = ' + str(max_crawl_id)

setup_db_tables(conn, myprefix)  # create msc tables if they do not exist

# Run data collection
new_crawl_id = 0
if runDataCollection == 1:
    sites = get_site_urls_to_extract()
    msc_UseOpenWPM.extract_via_openwpm(sites)
    print 'Data collection completed for:'
    for site in sites:
        print '  - ' + site
    new_crawl_id = conn.execute('SELECT  MAX(crawl_id) FROM crawl').fetchone()[0]
    print 'All data extraction completed for new crawl: {}'.format(new_crawl_id)

# extract parameters into parameter table
if do_extractParameters == 1 and new_crawl_id > 0:
    extract_parameters(conn, new_crawl_id, myprefix)

# close database connection
conn.close()

print '-done-'
