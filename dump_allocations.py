from utils import *

def dump(instrumentId=None, url=None, token=None, directory=None):
    # get all allocations for a given instrument
    status, allocations = get_allocations(instrumentId, url, token)
    if status == 200:
        data_to_yaml = {
            "allocations": allocations,
        }

        print(f"Saving data to '{directory}/data.yaml'")
        dict_to_yaml(data_to_yaml, f"{directory}/data.yaml")

    else:
        print(f"Error: no allocations found using instrumendId: {instrumentId}")

def main():
    parser = argparse.ArgumentParser(description="Dump SkyPortal allocations.")
    parser.add_argument("--instrumentId", help="The id of the instrument for which we are getting allocations", type=int)
    parser.add_argument("--url", help="The url of the Skyportal instance.", type=str)
    parser.add_argument("--token", help="The token of the Skyportal instance.", type=str)
    parser.add_argument("--use_config", help="Use config file to get parameters. Use it if you want to use the config file rather than providing parameters in the command line.", action="store_true")
    parser.add_argument("--directory", help="Directory to save results to. If not provided, a random directory name in results/ will be used.", type=str)
    args = parser.parse_args()

    use_config = args.use_config
    if use_config is True:
        try:
            config = yaml_to_dict("config.yaml")
            instrumentId = config["instrumentId"]
            url = config["skyportal_url"]
            token = config["skyportal_token"]
        except KeyError as e:
            print("Error: {} not found in config file.".format(e))
            return
    else:
        instrumentId = args.instrumentId
        url = args.url
        token = args.token


    if args.directory is None:
        dir = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
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

    dump(instrumentId, url, token, directory)

if __name__ == "__main__":
    main()
