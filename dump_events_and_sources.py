from utils import *
        
def dump(localizationDateobs: str = None, localizationName: str = None, startDate: str = None, endDate: str = None, localizationCumprob: float = 0.95, numberDetections: int = 2, numPerPage: int = 100, url: str = None, token: str = None, whitelisted: bool = False, directory: str = None):
    """
    Dump the data to yaml files.
    """

    print("Fetching sources and photometry... Please wait")
    status, data = get_all_sources_and_phot(localizationDateobs, localizationName, startDate, endDate, localizationCumprob, numberDetections, numPerPage, url, token, whitelisted)
    status = 200
    if status == 200 or status == 500:
        print("Found {} sources".format(len(data)))
        print("Formatting photometry...")
        sources, photometry_ref, instrument_ids = seperate_sources_from_phot(data, directory)
        
        print("Fetching groups, instruments and telescopes from Skyportal...")
        
        status, instruments = get_instruments_from_ids(instrument_ids, url, token)
        telescope_ids = list(set([instrument['telescope_id'] for instrument in instruments]))
        status, telescopes = get_telescopes_from_ids(telescope_ids, url, token)

        print("Fetching gcn notice from Skyportal...")
        status, tags, notice = get_gcnevent_data(localizationDateobs, localizationName, url, token)
        if status != 200:
            print("Error getting gcn data")
            return
        gcn_event = {}
        if notice is None:
            skymap_data = get_skymap(localizationDateobs, localizationName, url, token)
            if skymap_data is None:
                print("Error getting skymap")
                return
            filename = f'{directory}/{localizationName}.fits'
            hp.fitsfunc.write_map(filename, skymap_data, overwrite=True, nest=False, column_names=['PROB']) # column_names=['UNIQ', 'PROBDENSITY', 'DISTMU', 'DISTSIGMA', 'DISTNORM']
            gcn_event = {'dateobs': localizationDateobs, 'skymap': os.path.abspath(filename), 'tags': tags}
        else:
            # save the notice to a file
            filename = f'{directory}/{localizationName}.txt'
            with open(filename, 'w') as f:
                f.write(notice)
            gcn_event = {'xml': os.path.abspath(filename)}

        
        print("Formatting sources, instruments and telescopes...")

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

        sources = [formattedSource(source) for source in sources]
        photometry_ref = [formattedPhotRef(phot_ref, instrument_yaml_ids) for phot_ref in photometry_ref]

        data_to_yaml = {
            "telescope": telescopes,
            "instrument": instruments,
            "sources": sources,
            "photometry": photometry_ref,
            "gcn_event": [gcn_event]
        }

        print(f"Saving data to '{directory}/data.yaml'")
        dict_to_yaml(data_to_yaml, f"{directory}/data.yaml")

        params = {
            "localizationDateobs": localizationDateobs,
            "localizationName": localizationName,
            "startDate": startDate,
            "endDate": endDate,
            "numPerPage": numPerPage,
            "url": url,
            "token": token

        }
        dict_to_yaml(params, f"{directory}/config_used.yaml")
        print("Done! Now, you can load the results in a Skyportal instance.")
    
def main():
    parser = argparse.ArgumentParser(description="Dump SkyPortal sources found in a GCN Event, and their photometry.")
    parser.add_argument("--localizationDateobs", help="Dateobs of the localization/event.", type=str)
    parser.add_argument("--localizationName", help="Name of the localization.", type=str)
    parser.add_argument("--startDate", help="First detection of the source after this date.", type=str)
    parser.add_argument("--endDate", help="Last detection of the source before this date.", type=str)
    parser.add_argument('--localizationCumprob', help="Cumulative probability. To keep sources in the Nth most probable region. Default is 0.95", type=float, default=0.95)
    parser.add_argument("--numberDetections", help="Minimum number of detections for the sources. Default is 2.", type=int, default=2)
    parser.add_argument("--numPerPage", help="Number of sources to query at once. Default is 100.", type=int, default=100)
    parser.add_argument("--url", help="The url of the Skyportal instance.", type=str)
    parser.add_argument("--token", help="The token of the Skyportal instance.", type=str)
    parser.add_argument("--whitelisted", help="IP whitelisted on SkyPortal, no api calls limitation.", action="store_true")
    parser.add_argument("--use_config", help="Use config file to get parameters. Use it if you want to use the config file rather than providing parameters in the command line.", action="store_true")
    parser.add_argument("--directory", help="Directory to save results to. If not provided, a random directory name in results/ will be used.", type=str)
    args = parser.parse_args()

    use_config = args.use_config
    if use_config is True:
        try:
            config = yaml_to_dict("config.yaml")
            localizationDateobs = config["localizationDateobs"]
            localizationName = config["localizationName"]
            startDate = config["startDate"]
            endDate = config["endDate"]
            localizationCumprob= config["localizationCumprob"]
            numberDetections = config["numberDetections"]
            numPerPage = config["numPerPage"]
            whitelisted = config["whitelisted"]
            url = config["skyportal_url"]
            token = config["skyportal_token"]
        except KeyError as e:
            print("Error: {} not found in config file.".format(e))
            return
    else:
        localizationDateobs = args.localizationDateobs
        localizationName = args.localizationName
        startDate = args.startDate
        endDate = args.endDate
        localizationCumprob = args.localizationCumprob
        numberDetections = args.numberDetections
        numPerPage = args.numPerPage
        whitelisted = args.whitelisted
        url = args.url
        token = args.token

    missing_params = []
    if localizationDateobs is None:
        missing_params.append("localizationDateobs")
    if localizationName is None:
        missing_params.append("localizationName")
    if startDate is None:
        missing_params.append("startDate")
    if endDate is None:
        missing_params.append("endDate")
    if localizationCumprob is None:
        missing_params.append("localizationCumprob")
    if numberDetections is None:
        missing_params.append("numberDetections")
    if url is None:
        missing_params.append("url")
    if token is None:
        missing_params.append("token")

    if len(missing_params) > 0:
        print("Error: the following parameters are missing: {}".format(', '.join(missing_params)))
        return

    if args.directory is None:
        directory = f"results/{localizationDateobs}"
        counter = 1
        while os.path.exists(directory):
            directory = f"results/{localizationDateobs}_{counter}"
            counter += 1
        if not os.path.exists("results"):
            os.makedirs("results")
        if not os.path.exists(directory):
            os.makedirs(directory)
    else:
        directory = args.directory
        if not os.path.exists(directory):
            os.makedirs(directory)
    if not os.path.exists(directory):
        os.makedirs(directory)
    if not os.path.exists("{}/photometry".format(directory)):
        os.makedirs("{}/photometry".format(directory))
    
    dump(localizationDateobs, localizationName, startDate, endDate, localizationCumprob, numberDetections, numPerPage, url, token, whitelisted, directory)

if __name__ == "__main__":
    main()