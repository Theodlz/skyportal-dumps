import os
import time
import uuid
import csv
import yaml
import requests
import argparse
import numpy as np
import healpy as hp
import re
from datetime import datetime

group_fields = ['name']
telescope_fields = ['name', 'nickname', 'lat', 'lon', 'elevation', 'diameter', 'robotic', 'fixed_location', 'skycam_link', 'weather_link']
instrument_fields = ['name', 'type', 'band', 'telescope_id', 'filters', 'api_classname', 'api_classname_obsplan', 'treasuremap_id', 'sensitivity_data']
source_fields = ['id', 'ra', 'dec', 'origin', 'alias', 'group_ids', 'redshift']
photometry_fields = ['mjd', 'filter', 'mag', 'magerr', 'magsys', 'limiting_mag', 'ra', 'dec', 'ra_unc', 'dec_unc', 'origin']
photometry_ref_fields = ['obj_id', 'instrument_id', 'group_ids', 'file']
followups_fields = ['last_modified_by_id', 'obj_id', 'payload', 'status', 'allocation_id', 'created_at', 'id', 'modified', 'requester_id']

class MyDumper(yaml.SafeDumper):
    def write_line_break(self, data=None):
        super().write_line_break(data)

        if len(self.indents) == 1 or len(self.indents) == 2:
            super().write_line_break()

def yaml_to_dict(file_path):
    """
    Open a config file and return a dictionary containing the configuration.

    Arguments
    ----------
        file_path : str
            path to the yaml file

    Returns
    ----------
        config : dict
            dictionary containing the date from the yaml file
    """

    with open(file_path, "r") as stream:
        try:
            conf = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            raise exc
    return conf

def dict_to_yaml(dict, file_path):
    """
    Write a dictionary to a yaml file.

    Arguments
    ----------
        dict : dict
            dictionary to write to the yaml file
        file_path : str
            path to the yaml file

    Returns
    ----------
        None
    """
    with open(
        file_path,
        "w",
    ) as stream:
        try:
            yaml.dump(dict, stream, Dumper=MyDumper, sort_keys=False, default_flow_style=False)
        except yaml.YAMLError as exc:
            raise exc

def api(
    method,
    endpoint,
    data=None,
    params=None,
    token=None,
):
    """
    Make an API call to skyportal

    Arguments
    ----------
        method : str
            HTTP method to use
        endpoint : str
            Endpoint to call
        data : dict
            Data to send with the request
        token : str
            Skyportal token

    Returns
    ----------
        response : requests.Response
            Response from skyportal

    """
    if params:
        # add params in the url
        endpoint += "?"
        for key, value in params.items():
            endpoint += f"{key}={value}&"
        endpoint = endpoint[:-1]
    
    headers = {"Authorization": f"token {token}"}
    response = requests.request(method, endpoint, json=data, headers=headers)
    return response

def formattedInstrument(instrument, telescope_yaml_ids: dict = None):
    """
    Keep only the fields that are needed.
    """
    formatted_instrument = {}
    for field in instrument_fields:
        if field in instrument:
            formatted_instrument[field] = instrument[field]
        else:
            formatted_instrument[field] = None

    formatted_instrument['=id'] = instrument['name'].strip()
    if telescope_yaml_ids:
        formatted_instrument['telescope_id'] = f"={telescope_yaml_ids[instrument['telescope_id']]}"
    
    return formatted_instrument

def formattedTelescope(telescope):
    """
    Keep only the fields that are needed.
    """
    formatted_telescope = {}
    for field in telescope_fields:
        if field in telescope:
            formatted_telescope[field] = telescope[field]
        else:
            formatted_telescope[field] = None

    formatted_telescope['=id'] = telescope['nickname'].strip()

    return formatted_telescope

def formattedGroup(group):
    """
    Keep only the fields that are needed.
    """
    formatted_group = {}
    for field in group_fields:
        if field in group:
            formatted_group[field] = group[field]
        else:
            formatted_group[field] = None

    formatted_group['=id'] = group['name'].strip()

    return formatted_group

def get_all_analysis_services(url: str = None, token: str = None):
    analysis_services = api("GET", f"{url}/api/analysis_service", token=token)

    data = []
    if analysis_services.status_code == 200:
        data = analysis_services.json()["data"]
    return analysis_services.status_code, data

def get_analysis_service(name: str = None, url: str = None, token: str = None):
    status, analysis_services = get_all_analysis_services(url=url, token=token)
    data = {}
    if status == 200:
        for analysis_service in analysis_services:
            if analysis_service["name"] == name:
                data = analysis_service
                break
    return status, data

def get_analysis_from_source(source_id: str = None, url: str = None, token: str = None):
    analysis = api("GET", f"{url}/api/obj/analysis?objID={source_id}", token=token)
    data = []
    if analysis.status_code == 200:
        data = analysis.json()["data"]
    return analysis.status_code, data

def start_nmma_analysis(source_id: str = None, analysis_service_id: int = None, url: str = None, token: str = None):
    params= {
        "analysis_parameters": {
            "source": "Me2017"
        },
        "group_ids": [2],
        "show_corner": True,
        "show_parameters": True,
        "show_plots": True
    }
    analysis = api("POST", f"{url}/api/obj/{source_id}/analysis/{analysis_service_id}", data=params, token=token)
    return analysis.status_code

def get_gcnevent(localizationDateobs: str = None, url: str = None, token: str = None):
    gcn_event = api("GET", f"{url}/api/gcn_event/{localizationDateobs}", token=token)

    data = {}
    if gcn_event.status_code == 200:
        data = gcn_event.json()["data"]
    return gcn_event.status_code, data

def get_gcnevent_data(localizationDateobs: str = None, localizationName: str = None, url: str = None, token: str = None):
    status, gcn_event = get_gcnevent(localizationDateobs, url, token)
    if status != 200:
        print(f"Error {status} when getting gcn_event for {localizationDateobs}")
        return status, None, None
    localizationNames = [localization["localization_name"] for localization in gcn_event["localizations"]]
    if localizationName not in localizationNames:
        print(f"{localizationName} not in {localizationNames}")
        return 404, None, None

    notices = gcn_event['gcn_notices']
    # keep notices that have a "content" field
    notices = [notice for notice in notices if "content" in notice]
    # check if the localization name is like: float_float_float (e.g. -0.5_0.5_0.5) using regex
    if re.match(r"^-?\d+\.?\d*_-?\d+\.?\d*_-?\d+\.?\d*$", localizationName):
        ra, dec, radius = localizationName.split("_")
        # the localization is a fits file
        position_notices = [notice for notice in notices if '<Position2D unit="deg">' in notice["content"]]
        for notice in position_notices:
            notice_ra = notice["content"].split('<C1>')[1].split('</C1>')[0]
            notice_dec = notice["content"].split('<C2>')[1].split('</C2>')[0]
            notice_radius = notice["content"].split('<Error2Radius>')[1].split('</Error2Radius>')[0]
            if notice_ra in ra and notice_dec in dec and notice_radius in radius:
                return 200, gcn_event['tags'], notice["content"]
    # check if the localization name is a fits file
    extensions = ['.fit', '.fits', '.gz']
    if any(extension in localizationName for extension in extensions):
        fits_notices = [notice for notice in notices if '<Param name="skymap_fits"' in notice["content"]]
        for notice in fits_notices:
            notice_file = notice["content"].split('<Param name="skymap_fits"')[1].split('</Param>')[0]
            if localizationName in notice_file:
                return 200, gcn_event['tags'], notice["content"]

    return 200, gcn_event['tags'], None

def get_skymap(localizationDateobs: str = None, localizationName: str = None, url: str = None, token: str = None):
    params = {"include2DMap": True}
    localization = api("GET", f"{url}/api/localization/{localizationDateobs}/name/{localizationName}", params=params, token=token)
    data = np.array([])
    if localization.status_code == 200:
        data = np.array(localization.json()['data']["flat_2d"])
    return data

def get_telescopes(url: str = None, token: str = None):
    """
    Get all telescopes from skyportal using its API

    Arguments
    ----------
        url : str
            Skyportal url
        token : str
            Skyportal token
    Returns
    ----------
        status_code : int
            HTTP status code
        data : list
            List of telescopes
    """
    telescopes = api("GET", f"{url}/api/telescope", token=token)
    data = []
    if telescopes.status_code == 200:
        data = telescopes.json()["data"]
    return telescopes.status_code, data

def get_telescopes_from_ids(telescope_ids: list = None, url: str = None, token: str = None):
    """
    Get telescopes from skyportal using its API

    Arguments
    ----------
        telescope_ids : list
            List of telescope ids
        url : str
            Skyportal url
        token : str
            Skyportal token
    Returns
    ----------
        status_code : int
            HTTP status code
        data : list
            List of telescopes
    """
    status, all_telescopes = get_telescopes(url, token)
    if status == 200:
        telescopes = [telescope for telescope in all_telescopes if telescope['id'] in telescope_ids]
    else:
        telescopes = []
    
    return status, telescopes

def get_all_observations(telescopeName: str = None, instrumentName: str = None, localizationDateobs: str = None, localizationName: str = None, startDate: str = None, endDate: str = None, localizationCumprob: float = 0.95, numPerPage: int = 1000, pageNumber: int = 1, returnStatistics = False,  url: str = None, token: str = None, whitelisted: bool = False):
    """
    Get all observation ids from skyportal using its API

    Arguments
    ----------
        telescopeName : str
            Telescope name
        instrumentName : str
            Instrument name
        startDate : str
            Start date of the observation
        end_date : str
            End date of the observation
        localizationDateobs : str
            Dateobs of the localization
        localizationName : str
            Name of the localization
        returnStatistics : bool
            Boolean indicating whether to include integrated probability and area. Defaults to false.
        numPerPage : int
            Number of sources per page
        url : str
            Skyportal url
        token : str
            Skyportal token

    Returns
    ----------
        status_code : int
            HTTP status code
        data : list
            List of observation ids
    """
    request_counter = 0
    finished = False
    pageNumber = 1
    observations = [] 
    while finished == False:
        status_code, data = get_observations(telescopeName = telescopeName, instrumentName = instrumentName, localizationDateobs = localizationDateobs , localizationName = localizationName, startDate = startDate, endDate = endDate, localizationCumprob = localizationCumprob, returnStatistics = returnStatistics, numPerPage = numPerPage, pageNumber = pageNumber, url = url, token = token)
        if status_code == 200:
            if len(data) < numPerPage:
                finished = True
            if len(observations) == 0:
                observations = data
                pageNumber += 1
            else:
                observations.extend(data)
                pageNumber += 1
        elif status_code == 500:
            finished = True
        else:
            finished = True
            print("Error getting observations")

        if whitelisted is False:
            request_counter += 1
            if request_counter > 10:
                time.sleep(1) 
                request_counter = 0

    return status_code, observations

def get_observations(telescopeName: str = None, instrumentName: str = None, localizationDateobs: str = None, localizationName: str = None, startDate: str = None, endDate: str = None, localizationCumprob: float = 0.95, numPerPage: int = 1000, pageNumber: int = 1, returnStatistics = False,  url: str = None, token: str = None):
    """
    Get all source ids from skyportal using its API

    Arguments
    ----------
        telescopeName : str
            Telescope name
        instrumentName : str
            Instrument name
        startDate : str
            Start date of the observation
        endDate : str
            End date of the observation
        localizationDateobs : str
            Dateobs of the localization
        localizationName : str
            Name of the localization
        returnStatistics : bool
            Boolean indicating whether to include integrated probability and area. Defaults to false.
        numPerPage : int
            Number of sources per page
        pageNumber : int
            Page to get
        url : str
            Skyportal url
        token : str
            Skyportal token

    Returns
    ----------
        status_code : int
            HTTP status code
        data : list
            List of observation ids
    """

    params = {'returnStatistics': returnStatistics}

    if numPerPage is not None:
        params["numPerPage"] = numPerPage
    if pageNumber is not None:
        params["pageNumber"] = pageNumber
    if startDate is not None:
        params["startDate"] = startDate
    if endDate is not None:
        params["endDate"] = endDate
    if localizationCumprob is not None:
        params["localizationCumprob"] = localizationCumprob
    if localizationDateobs is not None:
        params["localizationDateobs"] = localizationDateobs
    if localizationName is not None:
        params["localizationName"] = localizationName
    if telescopeName is not None:
        params["telescopeName"] = telescopeName
    if instrumentName is not None:
        params["instrumentName"] = instrumentName

    observations = api("GET", f"{url}/api/observation", params=params, token=token) 
    data = [] 
    if observations.status_code == 200:
        data = observations.json()["data"]
    return observations.status_code, data

def retrieve_observations(data: dict = {}, url: str = None, token: str = None):
    """
    Retrieve observations from an external API

    Arguments
    ----------
        params : dict
            Parameter dictionary for request
        url : str
            Skyportal url
        token : str
            Skyportal token
    Returns
    ----------
        status_code : int
            HTTP status code
        data : list
            List of allocations
    """
    status_code = api("POST", f"{url}/api/observation/external_api", token=token, data=data)
    return status_code

def get_allocations(url: str = None, token: str = None):
    """
    Get all allocations from skyportal using its API

    Arguments
    ----------
        url : str
            Skyportal url
        token : str
            Skyportal token
    Returns
    ----------
        status_code : int
            HTTP status code
        data : list
            List of allocations
    """
    allocations = api("GET", f"{url}/api/allocation", token=token)
    data = []
    if allocations.status_code == 200:
        data = allocations.json()["data"]
    return allocations.status_code, data

def get_instruments(url: str = None, token: str = None):
    """
    Get all instruments from skyportal using its API

    Arguments
    ----------
        url : str
            Skyportal url
        token : str
            Skyportal token
    Returns
    ----------
        status_code : int
            HTTP status code
        data : list
            List of instruments
    """
    instruments = api("GET", f"{url}/api/instrument", token=token)
    data = []
    if instruments.status_code == 200:
        data = instruments.json()["data"]
    return instruments.status_code, data

def get_instruments_from_ids(instrument_ids: list = None, url: str = None, token: str = None):
    """
    Get all instruments from skyportal using its API

    Arguments
    ----------
        instrument_ids : list
            List of instrument ids
        url : str
            Skyportal url
        token : str
            Skyportal token
    Returns
    ----------
        status_code : int
            HTTP status code
        data : list
            List of instruments
    """
    status, all_instruments = get_instruments(url, token)
    if status == 200:
        instruments = [instrument for instrument in all_instruments if instrument['id'] in instrument_ids]
    else:
        instruments = []
    
    return status, instruments

def get_groups(url: str = None, token: str = None):
    """
    Get all groups from skyportal using its API

    Arguments
    ----------
        url : str
            Skyportal url
        token : str
            Skyportal token
    Returns
    ----------
        status_code : int
            HTTP status code
        data : list
            List of groups
    """
    params = {
        "includeSingleUserGroups": False,
    }
    groups = api("GET", f"{url}/api/groups", params=params, token=token)
    data = []
    if groups.status_code == 200:
        data = groups.json()["data"]['all_groups']
    return groups.status_code, data

def get_groups_from_ids(group_ids: list = None, url: str = None, token: str = None):
    """
    Get all groups from skyportal using its API

    Arguments
    ----------
        group_ids : list
            List of group ids
        url : str
            Skyportal url
        token : str
            Skyportal token
    Returns
    ----------
        status_code : int
            HTTP status code
        data : list
            List of groups
    """
    status, all_groups = get_groups(url, token)
    if status == 200:
        groups = [group for group in all_groups if group['id'] in group_ids]
    else:
        groups = []
    
    return status, groups

def get_all_gcnevents(startDate: str = None, endDate: str = None, tagKeep: str = None, tagRemove: str = None, numPerPage: int = 100, url: str = None, token: str = None, whitelisted: bool = False):
    """
    Get all gcn event ids from skyportal using its API

    Arguments
    ----------
        startDate : str
            Start date of the observation
        end_date : str
            End date of the observation
        tagKeep : str
            Tag to match gcn event to
        tagRemove : str
            Tag to filter out
        numPerPage : int
            Number of sources per page
        url : str
            Skyportal url
        token : str
            Skyportal token

    Returns
    ----------
        status_code : int
            HTTP status code
        data : list
            List of source ids
    """
    request_counter = 0
    finished = False
    pageNumber = 1
    gcnevents = [] 
    while finished == False:
        status_code, data = get_gcnevents(startDate, endDate, tagKeep, tagRemove, numPerPage, pageNumber, url, token)
        if status_code == 200:
            if len(data) < numPerPage:
                finished = True
            if len(gcnevents) == 0:
                gcnevents = data
                pageNumber += 1
            else:
                gcnevents.extend(data) 
                pageNumber += 1
        elif status_code == 500:
            finished = True
        else:
            finished = True
            print("Error getting sources") 

        if whitelisted is False:
            request_counter += 1
            if request_counter > 10:
                time.sleep(1) 
                request_counter = 0

    return status_code, gcnevents

def get_gcnevents(startDate: str = None, endDate: str = None, tagKeep: str = None, tagRemove: str = None, numPerPage: int = 100, pageNumber: int = 1, url: str = None, token: str = None):
    """
    Get all gcn event ids from skyportal using its API

    Arguments
    ----------
        startDate : str
            Start date of the observation
        end_date : str
            End date of the observation
        tagKeep : str
            Tag to match gcn event to
        tagRemove : str
            Tag to filter out
        numPerPage : int
            Number of sources per page
        pageNumber : int
            Page to get
        url : str
            Skyportal url
        token : str
            Skyportal token

    Returns
    ----------
        status_code : int
            HTTP status code
        data : list
            List of source ids
    """

    params = {
        "sortBy": "dateobs",
        "sortOrder": "asc",
    }
    if numPerPage is not None:
        params["numPerPage"] = numPerPage
    if pageNumber is not None:
        params["pageNumber"] = pageNumber
    if startDate is not None:
        params["startDate"] = startDate
    if endDate is not None:
        params["endDate"] = endDate
    if tagKeep is not None:
        params["tagKeep"] = tagKeep
    if tagRemove is not None:
        params["tagRemove"] = tagRemove

    gcnevents = api("GET", f"{url}/api/gcn_event", params=params, token=token)
    data = [] 
    if gcnevents.status_code == 200:
        data = gcnevents.json()["data"]["events"]
    return gcnevents.status_code, data

def get_sources(localizationDateobs: str = None, localizationName: str = None, startDate: str = None, endDate: str = None, localizationCumprob: float = 0.95, numberDetections: int = 2, numPerPage: int = 100, pageNumber: int = 1, url: str = None, token: str = None):
    """
    Get all source ids from skyportal using its API

    Arguments
    ----------
        startDate : str
            Start date of the observation
        end_date : str
            End date of the observation
        localizationDateobs : str
            Dateobs of the localization
        localizationName : str
            Name of the localization
        
        numPerPage : int
            Number of sources per page
        pageNumber : int
            Page to get
        url : str
            Skyportal url
        token : str
            Skyportal token

    Returns
    ----------
        status_code : int
            HTTP status code
        data : list
            List of source ids
    """

    params = {
        "sortBy": "saved_at",
        "sortOrder": "desc",
    }
    if numPerPage is not None:
        params["numPerPage"] = numPerPage
    if pageNumber is not None:
        params["pageNumber"] = pageNumber
    if startDate is not None:
        params["startDate"] = startDate
    if endDate is not None:
        params["endDate"] = endDate
    if localizationCumprob is not None:
        params["localizationCumprob"] = localizationCumprob
    if numberDetections is not None:
        params["numberDetections"] = numberDetections
    if localizationDateobs is not None:
        params["localizationDateobs"] = localizationDateobs
    if localizationName is not None:
        params["localizationName"] = localizationName
    
    sources = api("GET", f"{url}/api/sources", params=params, token=token)
    data = []
    if sources.status_code == 200:
        data = sources.json()["data"]["sources"]
    return sources.status_code, data

def get_photometry(source_id: str = None, format: str = 'mag', url: str = None, token: str = None):
    """
    Get all photometry from skyportal using its API

    Arguments
    ----------
        source_id : str
            Source id to get the photometry from
    Returns
    ----------
        status_code : int
            HTTP status code
        data : list
            List of photometry
    """
    params = {
        "format": format,
    }

    photometry = api("GET", f"{url}/api/sources/{source_id}/photometry", params=params, token=token)
    data = []
    if photometry.status_code == 200:
        data = photometry.json()["data"]
        if len(data) == 0:
            print(f"No photometry found for source {source_id}")
    return photometry.status_code, data

def get_all_sources_and_phot(localizationDateobs: str = None, localizationName: str = None, startDate: str = None, endDate: str = None, localizationCumprob: float = 0.95, numberDetections: int = 2, numPerPage: int = 100, url: str = None, token: str = None, whitelisted: bool = False):
    """
    Get all source ids from skyportal using its API

    Arguments
    ----------
        startDate : str
            Start date of the observation
        end_date : str
            End date of the observation
        localizationDateobs : str
            Dateobs of the localization
        localizationName : str
            Name of the localization
        numPerPage : int
            Number of sources per page
        url : str
            Skyportal url
        token : str
            Skyportal token

    Returns
    ----------
        status_code : int
            HTTP status code
        data : list
            List of source ids
    """
    request_counter = 0
    finished = False
    pageNumber = 1
    sources = []
    while finished == False:
        status_code, data = get_sources(localizationDateobs, localizationName, startDate, endDate, localizationCumprob, numberDetections, numPerPage, pageNumber, url, token)
        if status_code == 200:
            if len(data) < numPerPage:
                finished = True
            if len(sources) == 0:
                sources = data
                pageNumber += 1
            else:
                sources.extend(data)
                pageNumber += 1
        elif status_code == 500:
            finished = True
        else:
            finished = True
            print("Error getting sources")

        if whitelisted is False:
            request_counter += 1
            if request_counter > 10:
                time.sleep(1)
                request_counter = 0

    if len(sources) > 0:
        # get the photometry for each source
        for source in sources:
            status_code, photometry = get_photometry(source["id"], url=url, token=token)
            if status_code == 200:
                source["photometry"] = photometry
            else:
                print("Error getting photometry of source {}".format(source["id"]))

            if whitelisted is False:
                request_counter += 1
                if request_counter > 10:
                    time.sleep(1)
                    request_counter = 0
            

    return status_code, sources

def formattedSource(source):
    """
    Keep only the fields that are needed.
    """
    formatted_source = {}
    for field in source_fields:
        if field in source:
            formatted_source[field] = source[field]
        else:
            formatted_source[field] = None
    formatted_source['group_ids'] = ["=public_group_id"]
    
    return formatted_source

def formattedPhot(photometry):
    """
    Keep only the fields that are needed.
    """
    formatted_phot = {}
    for field in photometry_fields:
        if field in photometry:
            formatted_phot[field] = photometry[field]
        else:
            formatted_phot[field] = None
    
    return formatted_phot

def formattedPhotRef(photometryRef, instrument_yaml_ids: dict = None):
    """
    Keep only the fields that are needed.
    """
    formatted_phot_ref = {}
    for field in photometry_ref_fields:
        if field in photometryRef:
            formatted_phot_ref[field] = photometryRef[field]
        else:
            formatted_phot_ref[field] = None

    formatted_phot_ref['group_ids'] = ["=public_group_id"]

    if instrument_yaml_ids is not None:
        formatted_phot_ref['instrument_id'] = f"={instrument_yaml_ids[photometryRef['instrument_id']]}"
    
    return formatted_phot_ref

def seperate_sources_from_phot(data: list, directory: str = None):
    """
    Separates sources and photometry

    Arguments
    ----------
        sources : list
            List of sources

    Returns
    ----------
        sources : list
            List of sources
        photometry : list
            List of photometry
    """
    instrument_ids_full_list = []
    source_list_to_yaml = []
    photometry_list_to_yaml = []
    for source in data:
        source_list_to_yaml.append(formattedSource(source))
        # seperate the photometry by instrument
        if len(source["photometry"]) > 0:
            photometry_dict = {}
            for phot in source["photometry"]:
                instrument_id = phot['instrument_id']
                if instrument_id not in photometry_dict.keys():
                    photometry_dict[instrument_id] = {
                        "instrument_name": phot['instrument_name'],
                        "photometry": []
                    }
                photometry_dict[instrument_id]["photometry"].append(formattedPhot(phot))

            for instrument in photometry_dict:
                    photometry_dict[instrument]["photometry"].sort(key=lambda x: x["mjd"])
                    # remove duplicates. A duplicate is when there is the same mjd, mag, magerr
                    temp_photometry = []
                    for phot in photometry_dict[instrument]["photometry"]:
                        if any(x["mjd"] == phot["mjd"] and x["mag"] == phot["mag"] and x["magerr"] == phot["magerr"] and x["limiting_mag"] == phot["limiting_mag"] and x["filter"] == phot["filter"] for x in temp_photometry):
                            continue
                        else:
                            temp_photometry.append(phot)
                    photometry_dict[instrument]["photometry"] = temp_photometry
                    filename = source['id'] + "_" + str(photometry_dict[instrument]['instrument_name']) + ".csv"
                    # save file
                    with open(f"{directory}/photometry/{filename}", 'w') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow(photometry_fields)
                        for phot in photometry_dict[instrument]["photometry"]:
                            writer.writerow([phot[field] for field in photometry_fields])
                    
                    photometry_list_to_yaml.append({
                        'obj_id': source['id'],
                        'instrument_id': instrument,
                        'group_ids': ["=public_group_id"],
                        'file': "photometry/" + filename
                    })

            instrument_ids_full_list.extend(list(photometry_dict.keys()))
            instrument_ids_full_list = list(set(instrument_ids_full_list))


    return source_list_to_yaml, photometry_list_to_yaml, instrument_ids_full_list

def formattedFollowupRequest(followupRequest):
    """
    Keep only the fields that are needed.
    """
    formatted_followup = {}
    for field in followups_fields:
        if field in followupRequest:
            formatted_followup[field] = followupRequest[field]
    
    return formatted_followup

def get_followup_requests(instrument_id: int = None, source_id: str = None, startDate: str = None, endDate: str = None, status: str = None, observationStartDate: str = None, observationEndDate: str = None, output_format: str = None, pageNumber: int = 1, numPerPage: int = 100, url: str = None, token: str = None):
    """
    Get the followup requests for a source.
    """

    endpoint = f"{url}/api/followup_request"

    if instrument_id is not None:
        endpoint += f"/schedule/{instrument_id}"
        
    if output_format not in [None, 'png', 'pdf', 'csv']:
        raise ValueError("If you specify an output format, it must be one of: 'png', 'pdf', 'csv'")

    params = {
        "pageNumber": pageNumber,
        "numPerPage": numPerPage,
    }

    if source_id is not None:
        params["source_id"] = source_id
    if startDate is not None:
        params["startDate"] = startDate
    if endDate is not None:
        params["endDate"] = endDate
    if status is not None:
        params["status"] = status
    if observationStartDate is not None:
        params["observationStartDate"] = observationStartDate
    if observationEndDate is not None:
        params["observationEndDate"] = observationEndDate
    if output_format is not None:
        params["output_format"] = output_format

    headers = {'Authorization': f'token {token}'}
    response = requests.get(endpoint, params=params, headers=headers)
    status = response.status_code
    if status == 200:
        if instrument_id:
            # retrieve the response, which is a pdf file
            filename = f"followup_requests_{instrument_id}.pdf"
            return status, filename, response.content
        else:
            return status, [] if response.json()["data"] is None else response.json()["data"], None
    else:
        return status, None, None

def get_all_followup_requests(instrument_id: int = None, source_id: str = None, startDate: str = None, endDate: str = None, status: str = None, observationStartDate: str = None, observationEndDate: str = None, output_format: str = None, url: str = None, token: str = None):
    """
    Get all the followup requests for a source.
    """

    pageNumber = 1
    numPerPage = 100
    all_followups = []
    if instrument_id is not None:
        status, filename, file_data = get_followup_requests(instrument_id, source_id, startDate, endDate, None, observationStartDate, observationEndDate, output_format, pageNumber, numPerPage, url, token)
        if status == 200:
            return status, filename, file_data
        else:
            return status, None, None
    else:
        status, followups, _ = get_followup_requests(instrument_id, source_id, startDate, endDate, None, observationStartDate, observationEndDate, output_format, pageNumber, numPerPage, url, token)
        if status != 200:
            return status, None, None
        else:
            all_followups = followups['followup_requests']
            totalMatches = followups['totalMatches']
            while int(pageNumber*numPerPage) < int(totalMatches):
                pageNumber += 1
                status, followups, _ = get_followup_requests(instrument_id, source_id, startDate, endDate, None, observationStartDate, observationEndDate, output_format, pageNumber, numPerPage, url, token)
                if status != 200:
                    return status, None, None
                else:
                    all_followups.extend(followups['followup_requests'])

            all_followups = [formattedFollowupRequest(followup) for followup in all_followups]
            return status, all_followups, totalMatches
