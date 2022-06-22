import requests
import os
import yaml
import csv

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
        "includePhotometry": True,
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

def get_all_sources(startDate: str = None, endDate: str = None, localizationDateobs: str = None, localizationName: str = None, numPerPage: int = 100, url: str = None, token: str = None):
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
    finished = False
    pageNumber = 1
    sources = []
    while finished == False:
        status_code, data = get_sources(startDate, endDate, localizationDateobs, localizationName, numPerPage, pageNumber, url, token)
        if status_code == 200:
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

    return status_code, sources

def seperate_sources_from_phot(data: list):
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
    # create a photometry folder if it doesn't exist
    if not os.path.exists("results"):
        os.makedirs("results")
    if not os.path.exists("results/photometry"):
        os.makedirs("results/photometry")

    source_keys = config['source_fields']
    phot_keys = config['photometry_fields']
    phot_file_keys = config['photometry_file_fields']
    sources = []
    photometries = []
    # for each source, only keep the keys in the source_keys list
    for source in data:
        sources.append({key: source[key] if key in source else None for key in source_keys})
        if 'group_ids' in source_keys:
            sources[-1]['group_ids'] = [group['id'] for group in source['groups']]
        # remove keys with None values
        sources[-1] = {key: value for key, value in sources[-1].items() if value is not None}
        # group by instrument id of the photometry
        photometry_grouped = {}
        for phot in source['photometry']:
            if phot['instrument_id'] not in photometry_grouped:
                photometry_grouped[phot['instrument_id']] = []
            photometry_grouped[phot['instrument_id']].extend(source['photometry'])
        for instrument_id, phot in photometry_grouped.items():
        # create a photometry object, with the fields from the phot_keys list, and a reference to a csv file with the fields from the phot_file_keys list
            photometry = {key: source[key] if key in source else None for key in phot_keys}
            photometry = photometry | {key: phot[0][key] if key in phot[0] else None for key in phot_keys}
            #remove keys with None values
            photometry = {key: value for key, value in photometry.items() if value is not None}
            # init the file as a dict with the keys from the phot_file_keys list and empty lists for the values
            file = {key: [] for key in phot_file_keys}
            for line in phot:
                # for each line, add the values to the file
                for key in phot_file_keys:
                    file[key].append(line[key])
            filename = source['id'] + "_" + str(instrument_id) + ".csv"
            # save file
            with open("results/photometry/"+ filename, 'w') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(file.keys())
                writer.writerows(zip(*file.values()))
            photometry['file'] = "photometry/"+filename
            photometries.append(photometry)
    return sources, photometries
        

def main():
    """
    Main function
    """

    localizationDateobs = config["localizationDateobs"]
    localizationName = config["localizationName"]
    startDate = config["startDate"]
    endDate = config["endDate"]
    numPerPage = config["numPerPage"]
    url = config["skyportal_url"]
    token = config["skyportal_token"]

    status, data = get_all_sources(startDate, endDate, localizationDateobs, localizationName, numPerPage, url, token)

    if status == 200 or status == 500:
        sources, photometry = seperate_sources_from_phot(data)
        # save sources and photometry in one yaml file
        sources_and_phot = {
            "sources": sources,
            "photometry": photometry
        }
        dict_to_yaml(sources_and_phot, 'results/'+localizationDateobs+'.yaml')
        print("Saved sources and photometry in results/"+localizationDateobs+".yaml")
    else:
        print("Error getting sources")

main()