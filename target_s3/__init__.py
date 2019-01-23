#!/usr/bin/env python3

import boto3
import argparse
import datetime
import io
import os
import sys
import json
import shutil
import pkg_resources
import singer

logger = singer.get_logger()
DATE_TO_UPLOAD_SEP="date_to_upload"

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def create_stream_to_record_map(input, config):
    stream_to_record_map = {}
    last_state = None
    for line in input:
        try:
            json_line = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in json_line:
            raise Exception(
                "Line is missing required key 'type': {}".format(line))
        t = json_line['type']

        if t == 'RECORD':
            if 'stream' not in json_line:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line))
            stream_name = json_line["stream"]
            
            if "line_date_field" in config:
                date_field = config["line_date_field"]
                stream_name = stream_name + DATE_TO_UPLOAD_SEP + json_line['record'][date_field]
            
            if "date" in config:
                date_field = config["date"]
                stream_name = stream_name + DATE_TO_UPLOAD_SEP + date_field
            
            add_to_stream_records(stream_to_record_map, stream_name, line)
        
        if t == 'STATE' and "state_file_path" in config:
            last_state = json_line

    return (stream_to_record_map, last_state)


def persist_stream_map(stream_map, tmp_path):
    for stream, lines in stream_map.items():
        save_and_upload_file(stream, lines, tmp_path)


def save_and_upload_file(stream, lines, tmp_path):
    path = tmp_path + stream
    with open(path, 'w') as f:
        for line in lines:
            f.write(line)
        logger.info("tmp file written " + path)


def add_to_stream_records(stream_map, stream_name, line):
    if stream_name not in stream_map:
        stream_map[stream_name] = []
    stream_map[stream_name].append(line)


def delete_tmp_dir(tmp_path):
    shutil.rmtree(tmp_path)
    logger.info("deleteing tmp dir " + tmp_path)


def upload_to_s3(tmp_path, config, s3):
    for f in os.listdir(tmp_path):
        
        if 'line_date_field' in config or 'date' in config:
            file_name = f.split(DATE_TO_UPLOAD_SEP)[0]
            upload_date = f.split(DATE_TO_UPLOAD_SEP)[1]
        else:
            file_name = f
            upload_date = datetime.datetime.today().strftime('%Y-%m-%d')
        
        s3_file_name = os.path.join(
            config['prefix'], config['client'], file_name, upload_date, file_name)
        
        logger.info('Uploading to s3: ' + s3_file_name)
        s3.upload_file(tmp_path + '/' + f, config['bucket'], s3_file_name)
        logger.info('Uploaded to s3: ' + s3_file_name)


def create_temp_dir():
    date = datetime.datetime.today().strftime('%Y-%m-%d-%H-%M-%s-%f')
    path = '/tmp/target-s3/' + date + '/'
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def persist_state(state, config):
    path = config["state_file_path"]
    state_dir = path.rsplit("/", 1)[0]
    if not os.path.exists(state_dir):
        os.makedirs(state_dir)

    with open(path, 'w') as f:
        f.write(json.dumps(state["value"]))
    logger.info("state file written " + path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    s3 = boto3.client('s3')
    tmp_path = create_temp_dir()

    if not args.config:
        logger.error("config is required")
        exit(1)

    with open(args.config) as input:
        config = json.load(input)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    stream_map, last_state = create_stream_to_record_map(input, config)

    persist_stream_map(stream_map, tmp_path)

    upload_to_s3(tmp_path, config, s3)
    delete_tmp_dir(tmp_path)

    if last_state is not None and "state_file_path" in config:
        persist_state(last_state, config)


if __name__ == '__main__':
    main()
