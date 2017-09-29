"""
Amended on:     Sat 21 May 2017
Amended by:         Waldu Woensdregt
Description:    This file was amended from the OpenWPM "demo.py" code file
                to serve as a proof of concept for the master research
                data collection objective which requires third-party 
                advertising parameters to be collected
"""

from automation import TaskManager

# >> code amended for msc POC (start) <<
NUM_BROWSERS = 1
sites = ['http://www.smallestwebsitetotheworld.com/',]
# >> code amended for msc POC (end) <<

# Loads the manager preference and 3 copies of the default browser dictionaries
manager_params, browser_params = TaskManager.load_default_params(NUM_BROWSERS)

# Update browser configuration (use this for per-browser settings)
for i in xrange(NUM_BROWSERS):
    browser_params[i]['disable_flash'] = False #Enable flash for all three browsers
# >> (msc) line below removed for POC <<
# browser_params[0]['headless'] = True #Launch only browser 0 headless

# Update TaskManager configuration (use this for crawl-wide settings)
manager_params['data_directory'] = '~/Desktop/'
manager_params['log_directory'] = '~/Desktop/'

# Instantiates the measurement platform
# Commands time out by default after 60 seconds
manager = TaskManager.TaskManager(manager_params, browser_params)

# Visits the sites with both browsers simultaneously
for site in sites:
    manager.get(site, index='**') # ** = synchronized browsers

# Shuts down the browsers and waits for the data to finish logging
manager.close()
