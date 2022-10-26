from utils import *

def dump(url: str = None, token: str = None, whitelisted: bool = False, directory: str = None):
    """
    Dump the data to yaml files.
    """

    print("Fetching telescopes and instruments... Please wait")
    status = 200
    if status == 200 or status == 500:
        
        print("instruments and telescopes from Skyportal...")
        
        status, instruments = get_instruments(url, token)

        status, telescopes = get_telescopes(url, token)

        all_allocations = []

        new_telescopes = []
        telescope_yaml_ids = {}

        # first remove duplicates (i.e telescopes with the same id)
        temp_telescopes = []
        for telescope in telescopes:
            if not any(t["id"] == telescope["id"] for t in temp_telescopes):
                temp_telescopes.append(telescope)
        telescopes = temp_telescopes

        for telescope in telescopes:
            formatted_telescope = formattedTelescope(telescope)
            new_telescopes.append(formatted_telescope)
            telescope_yaml_ids[telescope["id"]] = formatted_telescope["=id"]
        telescopes = new_telescopes
            
        new_instruments = []
        instrument_yaml_ids = {}

        # first remove duplicates (i.e instruments with the same id)
        temp_instruments = []
        for instrument in instruments:
            if not any(i["id"] == instrument["id"] for i in temp_instruments):
                temp_instruments.append(instrument)
        instruments = temp_instruments

        for i in range(len(instruments)):
            instrument = instruments[i]
            formatted_instrument = formattedInstrument(instrument, telescope_yaml_ids)
            new_instruments.append(formatted_instrument)
            instrument_yaml_ids[instrument["id"]] = formatted_instrument["=id"]
            status, allocations = get_allocations(instrument['id'], url, token)
            if status == 200:
                all_allocations.extend(allocations)
            if not whitelisted and i % 10 == 0:
                print("Fetching allocations... Please wait")
                time.sleep(1)

        all_allocations = [formattedAllocation(allocation, instrument_yaml_ids) for allocation in all_allocations]

        instruments = new_instruments

        data_to_yaml = {
            "groups": [public_group],
            "user": [public_user],
            "telescope": telescopes,
            "instrument": instruments,
            "allocation": all_allocations
        }

        print(f"Saving data to '{directory}/data.yaml'")
        dict_to_yaml(data_to_yaml, f"{directory}/data.yaml")

        print("Done! Now, you can load the results in a Skyportal instance.")
    
def main():
    parser = argparse.ArgumentParser(description="Dump SkyPortal telescopes and their instruments.")
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
            whitelisted = config["whitelisted"]
            url = config["skyportal_url"]
            token = config["skyportal_token"]
        except KeyError as e:
            print("Error: {} not found in config file.".format(e))
            return
    else:
        whitelisted = args.whitelisted
        url = args.url
        token = args.token

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
    
    dump(url, token, whitelisted, directory)

if __name__ == "__main__":
    main()