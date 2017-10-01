-- SQLite query to extract parameter counts per URL sub-domain
SELECT TopURL_subdomain,sub_domain as request_subdomain
	,SUM(CASE WHEN INSTR(sub_domain,TopURL_subdomain) = 0 THEN 1 ELSE 0 END) 'ThirdParty'
	,SUM(CASE WHEN INSTR(sub_domain,TopURL_subdomain) > 0 THEN 1 ELSE 0 END) 'Local'
	, count(1)as Tot
FROM http_responses resp
	INNER JOIN msc_param_values param on param.http_response_id = resp.id
	INNER JOIN
		(	-- get sub-domain out of top URL
			SELECT DISTINCT top_url as topurl
				,REPLACE(
					REPLACE(
						REPLACE(
							REPLACE(
								substr(top_url,0, 
									CASE WHEN INSTR(top_url,'.com') > 0 
										THEN INSTR(top_url,'.com')+4 
										ELSE (CASE WHEN INSTR(top_url,'.co.uk') 
													THEN INSTR(top_url,'.co.uk')+6 
													ELSE INSTR(top_url,'.tv')+3 END) 
										END)
								,'https','')
							,'http','')
						,'://','')
					,'www.','')
					as TopURL_subdomain
			FROM http_responses
			WHERE crawl_id = 75
		) TopURL_SubDomains ON TopURL_SubDomains.topurl = resp.top_url
WHERE crawl_id = 75
GROUP BY  TopURL_subdomain,sub_domain
ORDER BY TopURL_subdomain,sub_domain  
 
-- SQLite query to retrieve the parameter counts per AP sub-domain
SELECT  sub_domain as request_subdomain
	,adpub.ap_name as AdPlatform
	,pdom.pdom_domain as platform_domain
	,COUNT(1) as TotParams
	,COUNT(DISTINCT TopURL_subdomain) as NumberOfPages
	,COUNT(1)/COUNT(DISTINCT TopURL_subdomain) as AveParamPerPage
FROM http_responses resp
	INNER JOIN msc_param_values param on param.http_response_id = resp.id
	INNER JOIN
		(	-- get sub-domain out of top URL
			SELECT DISTINCT top_url as topurl
				,REPLACE(
					REPLACE(
						REPLACE(
							REPLACE(
								substr(top_url,0, 
									CASE WHEN INSTR(top_url,'.com') > 0 
										THEN INSTR(top_url,'.com')+4 
										ELSE (CASE WHEN INSTR(top_url,'.co.uk') 
													THEN INSTR(top_url,'.co.uk')+6 
													ELSE INSTR(top_url,'.tv')+3 END) 
										END)
								,'https','')
							,'http','')
						,'://','')
					,'www.','')
					as TopURL_subdomain
			FROM http_responses
			WHERE crawl_id = 75
		) TopURL_SubDomains ON TopURL_SubDomains.topurl = resp.top_url
	LEFT OUTER JOIN msc_ap_domains pdom 
		ON INSTR(param.sub_domain,pdom.pdom_domain) > 0
	LEFT OUTER JOIN msc_ad_platforms adpub ON adpub.ap_id = pdom.ap_id
WHERE crawl_id = 75
	AND INSTR(sub_domain,TopURL_subdomain) = 0
GROUP BY  sub_domain,adpub.ap_name,pdom.pdom_domain
HAVING COUNT(DISTINCT TopURL_subdomain)  > 3
ORDER BY count(1) DESC

-- SQLite code to add parameter key’s into “msc_parameters” table
INSERT INTO msc_parameters (par_key)
SELECT DISTINCT param_key
FROM msc_param_values pv
	INNER JOIN http_responses resp ON resp.id = pv.http_response_id
WHERE crawl_id = 75
 
-- SQLite code to populate advertisement platforms and their requester domains
-- add platforms into table msc_ad_ platforms
INSERT INTO msc_ad_platforms (ap_name,ap_main_url,ghostery_link) 
VALUES
	('Yahoo','https://advertising.yahoo.com/'
	 	,'https://apps.ghostery.com/en/apps/yahoo_analytics'), --1
	('Facebook','https://www.facebook.com/business/products/ads'
	 	,'https://apps.ghostery.com/en/apps/facebook'), --2
	('Google','https://www.google.com/intl/en_uk/ads/'
	 	,'https://apps.ghostery.com/en/apps/google_analytics'), --3
	('Krux Digital', 'https://www.krux.com/company/about-krux/'
	 	,'https://apps.ghostery.com/en/apps/krux_digital'), --4
	('Moat', 'http://www.moat.com/'
	 	,'https://apps.ghostery.com/en/apps/moat'), --5
	('AppNexus', 'https://www.appnexus.com/en'
	 	,'https://apps.ghostery.com/en/apps/appnexus'), --6
	('Index Exchange', 'http://www.indexexchange.com/'
	 	,'https://apps.ghostery.com/en/apps/index_exchange_(formerly_casale_media)'), --7
	('AOL Advertising', 'http://advertising.aol.com/'
	 	,'https://apps.ghostery.com/en/apps/advertising.com'), --8
	('Integral Ad Science', 'https://integralads.com/'
	 	,'https://apps.ghostery.com/en/apps/integral_ad_science'), --9	
	('Rubicon Project', 'http://rubiconproject.com/'
	 	,'https://apps.ghostery.com/en/apps/rubicon'), --10
	('Quantcast', 'http://www.quantcast.com'
	 	,'https://apps.ghostery.com/en/apps/quantcast'), --11
	('OpenX', 'http://www.openx.com/'
	 	,'https://apps.ghostery.com/en/apps/OpenX'), --12
	('Full Circle Studies', 'http://www.fullcirclestudies.com/home.aspx'
	 	,'https://apps.ghostery.com/en/apps/scorecard_research_beacon'), --13
	('IPONWEB', 'http://www.iponweb.com/'
	 	,'https://apps.ghostery.com/en/apps/bidswitch'), --14
	('BlueKai', 'http://www.oracle.com/us/corporate/acquisitions/bluekai/index.html'
	 	,'https://apps.ghostery.com/en/apps/bluekai'), --15
	('Amazon Associates', 'https://affiliate-program.amazon.com/'
	 	,'https://apps.ghostery.com/en/apps/amazon_associates'), --16
	('MediaMath', 'http://www.mediamath.com/'
	 	,'https://apps.ghostery.com/en/apps/mediamath'), --17
	('PubMatic', 'http://www.pubmatic.com/'
	 	,'https://apps.ghostery.com/en/apps/pubmatic'), --18
	('The Trade Desk', 'http://www.thetradedesk.com/'
	 	,'https://apps.ghostery.com/en/apps/tradedesk'), --19
	('Neustar PlatformOne', 'http://www.neustar.biz/marketing-solutions#.U-kEJZSwJTM'
	 	,'https://apps.ghostery.com/en/apps/aggregate_knowledge') --20
						
-- add plartform domains into table msc_ap_domains
INSERT INTO msc_ap_domains (ap_id,pdom_domain) 
VALUES 		
	(1,'yahoo.com'),(2,'facebook.com'),(2,'facebook.net'),(3,'doubleclick.net')
	,(3,'youtube.com'),(3,'googlesyndication.com'),(3,'google-analytics.com')
    ,(3,'googlevideo.com'),(3,'google.com'),(3,'googletagmanager.com'),(3,'googleapis.com')
    ,(3,'google.co.uk'),(4,'krxd.net'),(5,'moatads.com'),(6,'adnxs.com'),(7,'casalemedia.com')
	,(8,'advertising.com'),(9,'adsafeprotected.com'),(10,'rubiconproject.com')
	,(11,'quantserve.com'),(12,'openx.net'),(13,'scorecardresearch.com')
	,(14,'bidswitch.net'),(15,'bluekai.com'),(15,'bkrtx.com'),(16,'amazon-adsystem.com')
	,(17,'mathtag.com'),(18,'pubmatic.com'),(19,'adsrvr.org'),(20,'agkn.com')


-- SQLite code to populate sensitivity categories
INSERT INTO msc_sensitivity ("category", "description", "score") 
VALUES 
	('Public Knowledge', 'I do not care about this data', '1');
	('Insensitive', 'Not concerned about this', '2');
	('Sensitive', 'I am somewhat concerned about this', '3');
	('Private', 
	  'I am concerned about this and will want to know this is being collected', '4');
	('High Risk', 
	  'I would not freely allow this information to be collected by 3rd parties and 
	 	want explicit notification',  '5');
 

-- investigating user ID value persistence
SELECT 
	pub.ap_name as [Advertisement Platform]
	--,resp.top_url as Site_CollectedFrom
	 ,param_key
	,param_value
	,COUNT(DISTINCT par_val_id) as [Parameter Occurrences]
	,COUNT(DISTINCT sub_domain) as [AP SubDomains]
	,COUNT(DISTINCT resp.top_url) AS Sites
--select *	
FROM msc_param_values pval
	INNER JOIN http_responses resp ON resp.id = pval.http_response_id
	INNER JOIN msc_ap_domains pdom 
		ON INSTR(pval.sub_domain, pdom.pdom_domain) > 1
	INNER JOIN msc_ad_platforms pub on pub.ap_id = pdom.ap_id
	LEFT OUTER JOIN msc_known_values kv 
		on pval.param_value = CASE WHEN kv.knownvalue IS NULL THEN '-unknown-' ELSE kv.knownvalue END
WHERE resp.crawl_id = 75
	AND kv.id IS NULL
	AND param_key IN ('cid','cookie','p_user_id','uId','google_gid','_gid','ga_vid','ga_sid')
GROUP BY pub.ap_name 
	,param_key
	,param_value
HAVING COUNT(DISTINCT sub_domain) + COUNT(DISTINCT resp.top_url) > 2
ORDER BY COUNT(DISTINCT resp.top_url) DESC
	, COUNT(DISTINCT par_val_id) DESC
	, COUNT(DISTINCT sub_domain)  DESC


-- update AP IDs into msc_param_values based on platfornm domain mappings to sub-domains
UPDATE  msc_param_values
SET ap_id = 
	(
		SELECT pdom.ap_id
		FROM msc_ap_domains pdom 
		WHERE INSTR(msc_param_values.sub_domain, pdom.pdom_domain) > 1
	)

-- check whether update worked correctly (should return 0 if worked)
SELECT count(1) TotParams
	,SUM(CASE WHEN IFNULL(pval.ap_id,0)  <> pdom2.ap_id THEN 1 ELSE 0 END) as IncorrectMatch
	,SUM(CASE WHEN pval.ap_id  = pdom2.ap_id THEN 1 ELSE 0 END) as Matched
	,SUM(CASE WHEN IFNULL(pval.ap_id,-1) = -1  THEN 1 ELSE 0 END) as Unmatched
FROM msc_param_values pval
	INNER JOIN http_responses resp ON resp.id = pval.http_response_id
	LEFT OUTER JOIN msc_ap_domains pdom2 ON INSTR(pval.sub_domain, pdom2.pdom_domain) > 1
WHERE resp.crawl_id = 75


-- add known keys into msc_known_keys
INSERT INTO msc_known_keys 
	(knownkey, descr, pdom_id, sensitivity_id, is_personal,sensitivity_note)
VALUES
	('cookie','cookie value',1,3,1,'cookie'),
	('piggybackCookie'
		,'cookies from other scripts (PubMatic Developer Portal,n.d.)',26,3,1
		,'cookie'),
	('google_gid','Google User ID (Google Developers,2017a)',1,4,1,'Google ID'),
	('google_gid','Google User ID (Google Developers,2017a)',7,4,1,'Google ID'),
	('google_gid','Google User ID (Google Developers,2017a)',13,4,1,'Google ID'),
	('google_gid','Google User ID (Google Developers,2017a)',15,4,1,'Google ID'),
	('google_gid','Google User ID (Google Developers,2017a)',19,4,1,'Google ID'),
	('google_gid','Google User ID (Google Developers,2017a)',22,4,1,'Google ID'),
	('google_gid','Google User ID (Google Developers,2017a)',24,4,1,'Google ID'),
	('google_gid','Google User ID (Google Developers,2017a)',25,4,1,'Google ID'),
	('google_gid','Google User ID (Google Developers,2017a)',28,4,1,'Google ID'),
	('utmcc','multiple cookie values (Google Developers,2017b)',4,3,1
		,'Advertiser ID '),
	('utmsc','Screen color depth (Google Developers,2017b)',4,1,0
		,'similar to screen size (Google Developers,2017)'),
	('utmsr','Screen resolution (Google Developers,2017b)',4,1,0
		,'similar to screen size')

		
-- investigating user ID value persistence		
SELECT 		
	pub.ap_name as [Advertisement Platform]	
	 ,param_key	
	,param_value	
	,COUNT(DISTINCT par_val_id) as [Parameter Occurrences]	
	,COUNT(DISTINCT sub_domain) as [AP SubDomains]	
	,COUNT(DISTINCT resp.top_url) AS Websites	
FROM msc_param_values pval		
	INNER JOIN http_responses resp ON resp.id = pval.http_response_id	
	INNER JOIN msc_ap_domains pdom 	
		ON INSTR(pval.sub_domain, pdom.pdom_domain) > 1
	INNER JOIN msc_ad_platforms pub on pub.ap_id = pdom.ap_id	
	LEFT OUTER JOIN msc_known_values kv 	
		on pval.param_value = CASE WHEN kv.knownvalue IS NULL THEN '-unknown-' ELSE kv.knownvalue END
WHERE resp.crawl_id = 75		
	AND kv.id IS NULL	
	AND param_key IN ('cid','cookie','p_user_id','uId','google_gid','_gid','ga_vid','ga_sid','id')	
	AND param_value NOT IN ('wfocus','osdim','osdtos','sodar','')	
GROUP BY pub.ap_name 		
	,param_key	
	,param_value	
HAVING COUNT(DISTINCT sub_domain) + COUNT(DISTINCT resp.top_url) > 2		
--HAVING COUNT(DISTINCT resp.top_url) > 1		
ORDER BY COUNT(DISTINCT resp.top_url) DESC		
	, COUNT(DISTINCT par_val_id) DESC	
	, COUNT(DISTINCT sub_domain)  DESC	
		
-- populate msc_param_classification
INSERT INTO msc_param_classification
	(par_val_id,is_personal,sensitivity_id)
SELECT pval.par_val_id
	, CASE WHEN keys.id IS NOT NULL		
		THEN keys.is_personal
		ELSE val.is_personal
		END as is_personal
	, CASE WHEN keys.id IS NOT NULL
		THEN keys.sensitivity_id
		ELSE val.sensitivity_id
		END as is_personal
FROM msc_param_values pval
	INNER JOIN http_responses resp ON resp.id = pval.http_response_id
	INNER JOIN msc_ad_platforms pub ON pub.ap_id = pval.ap_id
	INNER JOIN msc_ap_domains pdom 
		ON INSTR(pval.sub_domain, pdom.pdom_domain) > 1
	LEFT OUTER JOIN msc_known_keys keys 
		ON keys.knownkey = pval.param_key
			AND keys.pdom_id = pdom.pdom_id
	LEFT OUTER JOIN msc_known_values val
		ON val.knownvalue = pval.param_value
WHERE resp.crawl_id = 75 
	AND (keys.id IS NOT NULL OR val.id IS NOT NULL)
		
-- total counts per parameter value	
SELECT   count(1) as Count	
	,SUM(case when ap.ap_id  IS NOT NULL THEN 1 ELSE 0 END) as AP_mapped
	,SUM(case when class.par_class_id  IS NOT NULL THEN 1 ELSE 0 END) as Classified
--SELECT * from  msc_param_values 
FROM http_responses resp	
	INNER JOIN msc_param_values param on param.http_response_id = resp.id
	LEFT OUTER JOIN msc_ad_platforms ap ON ap.ap_id = param.ap_id
	LEFT OUTER JOIN msc_param_classification class ON class.par_val_id = param.par_val_id
WHERE crawl_id = 75	
	
-- get known key info
SELECT ap.ap_name as 'Ad Platform (Owner)'
	,knownkey as 'Known Key', descr as 'Key Description'
	,CASE WHEN is_personal = 1 THEN 'Yes' ELSE 'No' END as 'Is PII?'
	, sens.category as 'Sensitivity Assigned'
	, keys.sensitivity_note as 'Sensitivity Reason'
FROM msc_known_keys keys
	INNER JOIN msc_sensitivity sens ON sens.sensitivity_id = keys.sensitivity_id
	INNER JOIN msc_ap_domains dom ON dom.pdom_id = keys.pdom_id
	INNER JOIN msc_ad_platforms ap ON ap.ap_id = dom.ap_id
ORDER BY ap.ap_name, knownkey		

-- get known value info
SELECT knownvalue as 'Known Value', descr as 'Value Description'
	,CASE WHEN is_personal = 1 THEN 'Yes' ELSE 'No' END as 'Is PII?'
	, sens.category as 'Sensitivity Assigned'
	, val.sensitivity_note as 'Sensitivity Reason'
FROM msc_known_values val
	INNER JOIN msc_sensitivity sens ON sens.sensitivity_id = val.sensitivity_id
ORDER BY knownvalue
			
-- Show parametesr for all APs with 100% sensitivity
SELECT pub.ap_name as AdPlatform
	,pval.param_key as 'Key'
	,pval.param_value as 'Value'
	,CASE WHEN is_personal = 1 THEN 'Yes' ELSE 'No' END as 'Is Personal'
	,sens.score as 'Sensitivity Rating'
	,sens.category as 'Sensitivity Category'
	, TopURL_subdomain as Website
--select *
FROM msc_param_values pval	
	INNER JOIN http_responses resp ON resp.id = pval.http_response_id
	INNER JOIN msc_ad_platforms pub ON pub.ap_id = pval.ap_id
	LEFT OUTER JOIN msc_param_classification pclass ON pclass.par_val_id = pval.par_val_id
	LEFT OUTER JOIN msc_sensitivity sens ON sens.sensitivity_id = pclass.sensitivity_id
	INNER JOIN												
		(	-- get sub-domain out of top URL										
			SELECT DISTINCT top_url as topurl										
				,REPLACE(									
					REPLACE(								
						REPLACE(							
							REPLACE(						
								substr(top_url,0,					
									CASE WHEN INSTR(top_url,'.com') > 0				
										THEN INSTR(top_url,'.com')+4			
										ELSE (CASE WHEN INSTR(top_url,'.co.uk')			
													THEN INSTR(top_url,'.co.uk')+6
													ELSE INSTR(top_url,'.tv')+3 END)
										END)			
								,'https','')					
							,'http','')						
						,'://','')							
					,'www.','')								
					as TopURL_subdomain								
			FROM http_responses										
			WHERE crawl_id = 75										
		) TopURL_SubDomains ON TopURL_SubDomains.topurl = resp.top_url	
WHERE resp.crawl_id = 75 	
	AND pub.ap_id IN (4)
	and pclass.par_val_id IS NOT NULL
	--AND sens.score = 5
ORDER BY pub.ap_name, pval.param_key,pval.param_value

			

															