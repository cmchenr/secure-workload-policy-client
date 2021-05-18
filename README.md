# Secure Workload Policy Client
Cisco Secure Workload is designed to share it's dynamic policy with 3rd party systems to analyze and enforce Secure Workload policy.

## Usage
### Command Line Arguments
```
$ python3 secure_workload_policy_client.py --help
usage: secure_workload_policy_client.py [-h] --cert_folder CERT_FOLDER
                                        [--scopes] [--filters] [--policies]
                                        [--loop] [--seek_to_last] [--json]
                                        [--yaml] [--save_to_file SAVE_TO_FILE]

Secure Workload - Kafka Policy Client

optional arguments:
  -h, --help            show this help message and exit
  --cert_folder CERT_FOLDER
                        Kafka Certificate Folder
  --scopes              Include scopes in output.
  --filters             Include filters in output.
  --policies            Include policies in output.
  --loop                Default behavior is to download a single policy update
                        and exit. This flag changes the behaviour to
                        continuously listen for policy updates.
  --seek_to_last        Certain scenarios can pause policy updates, which can
                        cause the script to hang. This flag seeks back in the
                        policy stream to retrieve the most recent update vs.
                        waiting for the next.
  --json                Set the output format to JSON.
  --yaml                Set the output format to YAML.
  --save_to_file SAVE_TO_FILE
                        Filename to save without extension.
```

### Example Use Case
```
python3 secure_workload_policy_client.py --cert_folder="./Policy-Stream-308/" --yaml --filters --scopes --policies --loop --seek_to_last --save_to_file="policy_export_10-1-2021"
```