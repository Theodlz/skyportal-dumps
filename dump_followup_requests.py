from utils import *

def dump(instrumentId=None, startDate=None, endDate=None, numPerPage=None, url=None, token=None, whitelisted=None, directory=None):
    if instrumentId is not None:
        print(f"Fetching follow-up requests schedule (as csv) for instrument_id: {instrumentId}... Please wait")
        status, filename, file_data = get_all_followup_requests(instrument_id=instrumentId, startDate=startDate, endDate=endDate, url=url, token=token)
        if status == 200:
            with open("{}/{}.csv".format(directory, filename), "wb") as f:
                f.write(file_data)
        else:
            print("Error: no follow-up requests found using these parameters")
    else:
        print(f"Fetching follow-up requests... Please wait")
        status, followups, totalMatches = get_all_followup_requests(instrument_id=instrumentId, startDate=startDate, endDate=endDate, url=url, token=token)
        if status == 200:
            data_to_yaml = {
            "followup_requests": followups,
            }

            print(f"Saving data to '{directory}/data.yaml'")
            dict_to_yaml(data_to_yaml, f"{directory}/data.yaml")
    

def main():
    parser = argparse.ArgumentParser(description="Dump SkyPortal follow-up requests.")
    parser.add_argument("--instrumentId", help="The id of the instrument for which we are getting follow-up requests", type=int)
    parser.add_argument("--startDate", help="First detection of the source after this date.", type=str)
    parser.add_argument("--endDate", help="Last detection of the source before this date.", type=str)
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
            if "instrumentId" in config:
                instrumentId = config["instrumentId"]
            else:
                instrumentId = None
            startDate = config["startDate"]
            endDate = config["endDate"]
            numPerPage = config["numPerPage"]
            url = config["skyportal_url"]
            token = config["skyportal_token"]
            whitelisted = config["whitelisted"]
        except KeyError as e:
            print("Error: {} not found in config file.".format(e))
            return
    else:
        instrumentId = args.instrumentId
        startDate = args.startDate
        endDate = args.endDate
        numPerPage = args.numPerPage
        url = args.url
        token = args.token
        whitelisted = args.whitelisted

    missing_params = []
    if url is None:
        missing_params.append("url")
    if token is None:
        missing_params.append("token")

    if len(missing_params) > 0:
        print("Error: the following parameters are missing: {}".format(', '.join(missing_params)))
        return

    dir = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    if args.directory is None:
        directory = f"results/{dir}"
        counter = 1
        while os.path.exists(directory):
            directory = f"results/{dir}_{counter}"
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
    
    dump(instrumentId, startDate, endDate, numPerPage, url, token, whitelisted, directory)

if __name__ == "__main__":
    main()