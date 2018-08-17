#!/usr/bin/env python
"""
WARNING: This script is experimental and not yet fully tested.

A script that pulls all the map/reduce documents from a given couchdb 
and compares it against the directory it is ran from.

The default behaviour is to only compare the output, but additional 
arguments can be used to either overwrite existing files and/or add 
missing files.
"""
import argparse
import requests
import os
import logging
import tempfile
import subprocess
import yaml
import coloredlogs
import pathlib

# Set up a logger with colored output
logger = logging.getLogger(__name__)
logger.propagate = False  # Otherwise the messages appeared twice
coloredlogs.install(level='INFO', logger=logger,
                    fmt='%(asctime)s %(levelname)s %(message)s')

def parse_settings(input_file):
    with open(input_file, 'r') as ifh:
        settings = yaml.safe_load(ifh)
    return settings['couch_server']

def compare_files(file_path, view, overwrite):
    file_contents = open(file_path).read()
    if view == file_contents:
        print('Success')
    else:
        temp_stage = tempfile.NamedTemporaryFile(delete = True)
        temp_stage.write(bytes(view, 'utf-8'))
        temp_stage.flush()

        logger.error("Diff detected for {}:".format(file_path))
        if overwrite:
            with open(file_path, 'w') as ofh:
                ofh.write(view)
        else:
            subprocess.run(["git", "diff", "-w", file_path, temp_stage.name])

def create_file(file_path, view):
    dirname = os.path.dirname(file_path)
    pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)
    with open(file_path, 'w') as ofh:
        ofh.write(view)

def main(view_dir, databases, settings_file, overwrite, add_new_files):
    base_url = parse_settings(settings_file)
    for database in databases:
        query_string = "_all_docs?startkey=%22_design/%22&endkey=%22_design0%22&include_docs=true" 
        query_url = "{}/{}/{}".format(base_url, database, query_string)
        resp = requests.get(query_url).json()
        if 'rows' not in resp:
            logger.error("No rows found in response for database {}."
                        "The script cannot handle this scenario yet.".format(database))
            continue
        if not os.path.isdir(os.path.join(view_dir, database)):
            logger.error('Directory with same name as database {} not found.'
                        'Please make sure your supplied <view_dir>:{} is correct'.format(database, view_dir))
            break
        for row in resp['rows']:
            document_id = row['doc']['_id']
            repo_dir = document_id.replace('_design/', '')
            for view_id, view_contents in row['doc']['views'].items():
                # Check for map file
                map_file_path = os.path.join(view_dir, database, repo_dir, "{}.map.js".format(view_id))
                if os.path.isfile(map_file_path):
                    compare_files(map_file_path, view_contents['map'], overwrite)
                else:
                    logger.error("Map file {} does not exist in repo!".format(map_file_path)) 
                    if add_new_files:
                        create_file(map_file_path, view_contents['map'])

                # Check for reduce file
                if 'reduce' in view_contents:
                    reduce_file_path = os.path.join(view_dir, database, repo_dir, "{}.reduce.js".format(view_id))
                    if os.path.isfile(reduce_file_path):
                        compare_files(reduce_file_path, view_contents['reduce'], overwrite)
                    else:
                        logger.error("Reduce file {} does not exist in repo!".format(reduce_file_path)) 
                        if add_new_files:
                            create_file(reduce_file_path, view_contents['reduce'])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('--view_dir', required=True, help='The directory/respository where the views are stored locally. '
                                         'Make sure to check in any local changes before running this script.')
    parser.add_argument('--statusdb_config', required=True, help='The genomics-status settings.yaml file.')
    parser.add_argument('--databases', required=True, nargs='*')
    parser.add_argument('--overwrite', action='store_true', help='This option will overwrite your local files if they are not identical')
    parser.add_argument('--add', action='store_true', help='Use this option if you would like to add scripts '
                                                            'which are missing in the repo')
    args = parser.parse_args()
    main(args.view_dir, args.databases, args.statusdb_config, args.overwrite, args.add)
