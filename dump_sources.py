import os
import time
import uuid
import csv
import yaml
import requests

source_fields = ['id', 'ra', 'dec', 'origin', 'alias', 'group_ids', 'redshift']
photometry_fields = ['mjd', 'filter', 'mag', 'magerr', 'magsys', 'limiting_mag', 'ra', 'dec', 'ra_unc', 'dec_unc', 'origin']

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

config = yaml_to_dict("config.yaml")

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

def get_sources(startDate: str = None, endDate: str = None, localizationDateobs: str = None, localizationName: str = None, numPerPage: int = None, pageNumber: int = None, url: str = None, token: str = None):
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
    return photometry.status_code, data

def get_all_sources_and_phot(startDate: str = None, endDate: str = None, localizationDateobs: str = None, localizationName: str = None, numPerPage: int = 100, url: str = None, token: str = None, whitelisted: bool = False):
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
    print("Fetching sources and photometry... Please wait")
    while finished == False:
        status_code, data = get_sources(startDate, endDate, localizationDateobs, localizationName, numPerPage, pageNumber, url, token)
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

    formatted_source['group_ids'] = [group['id'] for group in source['groups']]
    
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
    source_list_to_yaml = []
    photometry_list_to_yaml = []
    # for each source, only keep the keys in the source_keys list
    for source in data:
        source_list_to_yaml.append(formattedSource(source))
        # seperate the photometry by instrument and groups
        if len(source["photometry"]) > 0:
            photometry_dict = {}
            for phot in source["photometry"]:
                instrument_id = phot['instrument_id']
                group_ids = []
                for group in phot['groups']:
                    group_ids.append(group['id'])
                group_ids.sort()
                if instrument_id not in photometry_dict.keys():
                    photometry_dict[instrument_id] = {
                        "instrument_name": phot['instrument_name'],
                        "by_group": []
                    }

                if not any(d['group_ids'] == group_ids for d in photometry_dict[instrument_id]['by_group']):
                    photometry_dict[instrument_id]["by_group"].append({"group_ids": group_ids, "groups": [group['name'] for group in phot["groups"]], "photometry": [formattedPhot(phot)]})
                else:
                    for d in photometry_dict[instrument_id]["by_group"]:
                        if d["group_ids"] == group_ids:
                            d["photometry"].append(formattedPhot(phot))
                            break

            for instrument in photometry_dict:
                for by_group in photometry_dict[instrument]["by_group"]:
                    by_group["photometry"].sort(key=lambda x: x["mjd"])
                    filename = source['id'] + "_" + str(photometry_dict[instrument]['instrument_name']) + "_" + str(','.join([str(i) for i in by_group['group_ids']])) + ".csv"
                    # save file
                    with open("results/{}/photometry/".format(directory)+ filename, 'w') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow(photometry_fields)
                        for phot in by_group["photometry"]:
                            writer.writerow([phot[field] for field in photometry_fields])
                    
                    photometry_list_to_yaml.append({
                        'obj_id': source['id'],
                        'instrument_id': instrument,
                        'group_ids': by_group['group_ids'],
                        'file': "photometry/" + filename
                    })


    return source_list_to_yaml, photometry_list_to_yaml
        


def main():
    """
    Main function
    """
    directory = str(uuid.uuid1())
    if not os.path.exists("results"):
        os.makedirs("results")
    if not os.path.exists("results/{}".format(directory)):
        os.makedirs("results/{}".format(directory))
    if not os.path.exists("results/{}/photometry".format(directory)):
        os.makedirs("results/{}/photometry".format(directory))
    
    localizationDateobs, localizationName, startDate, endDate, numPerPage, whitelisted = None, None, None, None, 100, False
    if 'localizationDateobs' in config:
        localizationDateobs = config["localizationDateobs"]
    if 'localizationNam' in config:
        localizationName = config["localizationName"]
    if 'startDate' in config:
        startDate = config["startDate"]
    if 'endDate' in config:
        endDate = config["endDate"]
    if 'numPerPage' in config:
        numPerPage = config["numPerPage"]
    if 'whitelisted' in config:
        whitelisted = config["whitelisted"]
    url = config["skyportal_url"]
    token = config["skyportal_token"]

    status, data = get_all_sources_and_phot(startDate, endDate, localizationDateobs, localizationName, numPerPage, url, token, whitelisted)

    if status == 200 or status == 500 or status == 400:
        print("Found {} sources".format(len(data)))
        print("Saving sources... Please wait".format(directory))
        seperate_sources_from_phot(data, directory)
        sources, photometry = seperate_sources_from_phot(data, directory)
        # save sources and photometry in one yaml file
        sources_and_phot = {
            "sources": sources,
            "photometry": photometry
        }
        dict_to_yaml(sources_and_phot, 'results/'+directory+'/sources.yaml')
        # also save a separate yaml file containing the params used to get the sources
        params = {
            "localizationDateobs": localizationDateobs,
            "localizationName": localizationName,
            "startDate": startDate,
            "endDate": endDate,
            "numPerPage": numPerPage,
            "url": url,
            "token": token

        }
        dict_to_yaml(params, 'results/'+directory+'/config_used.yaml')
        print("Saved sources to results/{}/sources.yaml".format(directory))
    
main()