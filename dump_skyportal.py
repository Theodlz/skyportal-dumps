import os
import time
import uuid
import csv
import yaml
import requests
group_fields = ['name']
telescope_fields = ['name', 'nickname', 'lat', 'lon', 'elevation', 'diameter', 'robotic', 'skycam_link', 'weather_link']
instrument_fields = ['name', 'type', 'band', 'telescope_id', 'filters', 'api_classname', 'api_classname_obsplan', 'treasuremap_id', 'sensitivity_data']
source_fields = ['id', 'ra', 'dec', 'origin', 'alias', 'group_ids', 'redshift']
photometry_fields = ['mjd', 'filter', 'mag', 'magerr', 'magsys', 'limiting_mag', 'ra', 'dec', 'ra_unc', 'dec_unc', 'origin']
photometry_ref_fields = ['obj_id', 'instrument_id', 'group_ids', 'file']

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

def formattedSource(source, group_yaml_ids: dict = None):
    """
    Keep only the fields that are needed.
    """
    formatted_source = {}
    for field in source_fields:
        if field in source:
            formatted_source[field] = source[field]
        else:
            formatted_source[field] = None
    if group_yaml_ids is None:
        formatted_source['group_ids'] = [group['id'] for group in source['groups']]
    else:
        formatted_source['group_ids'] = []
        for group_id in source['group_ids']:
            if group_id in group_yaml_ids.keys():
                formatted_source['group_ids'].append(f"={group_yaml_ids[group_id]}")
    
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

def formattedPhotRef(photometryRef, group_yaml_ids: dict = None, instrument_yaml_ids: dict = None):
    """
    Keep only the fields that are needed.
    """
    formatted_phot_ref = {}
    for field in photometry_ref_fields:
        if field in photometryRef:
            formatted_phot_ref[field] = photometryRef[field]
        else:
            formatted_phot_ref[field] = None

    if group_yaml_ids is not None:
        formatted_phot_ref['group_ids'] = []
        for group_id in photometryRef['group_ids']:
            if group_id in group_yaml_ids.keys():
                formatted_phot_ref['group_ids'].append(f"={group_yaml_ids[group_id]}")

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
    group_ids_full_list = []
    source_list_to_yaml = []
    photometry_list_to_yaml = []
    # for each source, only keep the keys in the source_keys list
    for source in data:
        source_list_to_yaml.append(formattedSource(source))
        group_ids_full_list.extend(formattedSource(source)['group_ids'])
        group_ids_full_list = list(set(group_ids_full_list))
        # seperate the photometry by instrument and groups
        if len(source["photometry"]) > 0:
            photometry_dict = {}
            for phot in source["photometry"]:
                instrument_id = phot['instrument_id']
                group_ids = []
                for group in phot['groups']:
                    group_ids.append(group['id'])
                group_ids.sort()
                group_ids_full_list.extend(list(group_ids))
                group_ids_full_list = list(set(group_ids_full_list))
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

            instrument_ids_full_list.extend(list(photometry_dict.keys()))
            instrument_ids_full_list = list(set(instrument_ids_full_list))


    return source_list_to_yaml, photometry_list_to_yaml, instrument_ids_full_list, group_ids_full_list
        
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
    print("Fetching sources and photometry from Skyportal")
    status, data = get_all_sources_and_phot(startDate, endDate, localizationDateobs, localizationName, numPerPage, url, token, whitelisted)

    if status == 200 or status == 500 or status == 400:
        print("Found {} sources".format(len(data)))
        print("Formatting photometry...")
        sources, photometry_ref, instrument_ids, group_ids = seperate_sources_from_phot(data, directory)
        print("Fetching groups, instruments and telescopes from Skyportal...")
        status, instruments = get_instruments_from_ids(instrument_ids, url, token)
        telescope_ids = list(set([instrument['telescope_id'] for instrument in instruments]))
        status, telescopes = get_telescopes_from_ids(telescope_ids, url, token)
        status, groups = get_groups_from_ids(group_ids, url, token)
        print("Formatting sources, groups, instruments and telescopes...")
        #reformat the groups, instruments, and telescopes
        new_groups = []
        group_yaml_ids = {}
        for group in groups:
            if group['name'] == 'Sitewide Group':
                group_yaml_ids[group["id"]] = "public_group_id"
            else:
                formatted_group = formattedGroup(group)
                new_groups.append(formatted_group)
                group_yaml_ids[group["id"]] = formatted_group["=id"]

        print('\n')
        print(groups)
        print('\n')
        print(new_groups)
        print('\n')
        groups = new_groups

        new_telescopes = []
        telescope_yaml_ids = {}
        for telescope in telescopes:
            formatted_telescope = formattedTelescope(telescope)
            new_telescopes.append(formatted_telescope)
            telescope_yaml_ids[telescope["id"]] = formatted_telescope["=id"]
        telescopes = new_telescopes
            
        new_instruments = []
        instrument_yaml_ids = {}
        for instrument in instruments:
            formatted_instrument = formattedInstrument(instrument, telescope_yaml_ids)
            new_instruments.append(formatted_instrument)
            instrument_yaml_ids[instrument["id"]] = formatted_instrument["=id"]
        instruments = new_instruments

        sources = [formattedSource(source, group_yaml_ids) for source in sources]
        photometry_ref = [formattedPhotRef(phot_ref, group_yaml_ids, instrument_yaml_ids) for phot_ref in photometry_ref]

        # save sources and photometry in one yaml file
        data_to_yaml = {
            "groups": groups,
            "telescope": telescopes,
            "instrument": instruments,
            "sources": sources,
            "photometry": photometry_ref
        }
        print("Saving data to results/{}/data.yaml".format(directory))
        dict_to_yaml(data_to_yaml, "results/{}/data.yaml".format(directory))
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
        print("Done!")
    
main()