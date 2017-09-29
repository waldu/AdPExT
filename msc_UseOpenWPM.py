# -*- coding: utf-8 -*-
"""
Created on:     Sun May 21 05:09:40 2017
Author:         Waldu Woensdregt
Description:    Functions used by msc_CollectData.py to use OpenWPM package
"""
from automation import TaskManager


def extract_via_openwpm(sites):
    """
    Utilise the OpenWPM package to extract the browser parameters for 
    provided sites
    """

    print '########## OpenWPM (start) (Englehardt, 2016) ##########'
    # The number of browsers to use to extract the data
    num_of_browsers = 1

    # Loads the manager preference and 3 copies of the default browser dictionaries
    manager_params, browser_params = TaskManager.load_default_params(num_of_browsers)

    # Update browser configuration (use this for per-browser settings)
    for i in xrange(num_of_browsers):
        browser_params[i]['disable_flash'] = False  # Enable flash for all three browsers
    # browser_params[0]['headless'] = True #Launch only browser 0 headless

    # Update TaskManager configuration (use this for crawl-wide settings)
    manager_params['data_directory'] = '~/Desktop/'
    manager_params['log_directory'] = '~/Desktop/'

    # Instantiates the measurement platform
    # Commands time out by default after 60 seconds
    manager = TaskManager.TaskManager(manager_params, browser_params)

    # Visit the sites
    for site in sites:
        manager.get(site, index='**')  # ** = synchronized browsers

    # Shuts down the browsers and waits for the data to finish logging
    manager.close()
    print '########## OpenWPM (end) (Englehardt, 2016) ##########'
