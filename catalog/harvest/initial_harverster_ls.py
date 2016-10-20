#!/usr/bin/env python
# coding: utf8
# Created by sgebhardt at 30.08.16
# Copyright EOSS GmbH 2016
from api.eoss_api import Api
from manage.landsat_catalog import USGS_HTTP_SERVICE
from model.plain_models import USGSOrderContainer, GoogleLandsatContainer, S3PublicContainer, \
    Catalog_Dataset
from datetime import datetime
from utilities.web_utils import remote_file_exists


def landsat_harvester(in_csv):
    datasets = []
    header = False
    with open(in_csv, 'r') as f:
        for counter, line in enumerate(f):
            if header == False:
                header = True
            else:
                content_list = line.split(',')
                if (content_list[52] != 'LANDSAT_4'):
                    dataset = Catalog_Dataset()
                    dataset.entity_id = content_list[0]
                    dataset.sensor = content_list[1]
                    dataset.tile_identifier = '%03d%03d' % (int(content_list[6]), int(content_list[7]))
                    dataset.clouds = float(content_list[19])
                    dataset.daynight = str.lower(content_list[24])
                    if dataset.sensor == "LANDSAT_TM":
                        dataset.acq_time = datetime.strptime(content_list[29][:-6], '%Y:%j:%H:%M:%S')
                    else:
                        dataset.acq_time = datetime.strptime(content_list[29][:-8], '%Y:%j:%H:%M:%S')
                    dataset.level = content_list[53]

                    container = dict()
                    container['quicklook'] = content_list[5]
                    sensor = dataset.sensor
                    if sensor == "OLI_TIRS":
                        sensor = "LANDSAT_8"
                    container['metadata'] = USGS_HTTP_SERVICE % (
                        int(content_list[6]), int(content_list[6]), int(content_list[7]), int(content_list[7]), sensor,

                        dataset.acq_time.strftime("%Y-%m-%d"), dataset.acq_time.strftime("%Y-%m-%d"))

                    usgs = USGSOrderContainer()
                    usgs.link = content_list[54]
                    container.update(usgs.to_dict())

                    google = GoogleLandsatContainer()
                    google_sensors = {'OLI_TIRS': 'L8', 'LANDSAT_ETM_SLC_OFF': 'L7', 'LANDSAT_ETM': 'L7',
                                      'LANDSAT_TM': 'L5', 'TIRS': 'L8', 'OLI': 'L8'}
                    google_link = google.base % (
                        google_sensors[content_list[1]], int(content_list[6]), int(content_list[7]), content_list[0])
                    if remote_file_exists(google_link):
                        google.link = google_link
                        container.update(google.to_dict())

                    dataset.resources = container

                    datasets.append(dataset)
                else:
                    print 'skipping...'

                if (counter % 25000) == 0:
                    print counter

    return datasets


def landsat_harvester_line(lines):
    datasets = []
    header = False

    for counter, line in enumerate(lines):
        content_list = line.split(',')
        if (content_list[52] != 'LANDSAT_4'):
            dataset = Catalog_Dataset()
            dataset.entity_id = content_list[0]
            dataset.sensor = content_list[1]
            dataset.tile_identifier = '%03d%03d' % (int(content_list[6]), int(content_list[7]))
            dataset.clouds = float(content_list[19])
            dataset.daynight = str.lower(content_list[24])
            if dataset.sensor == "LANDSAT_TM":
                dataset.acq_time = datetime.strptime(content_list[29][:-6], '%Y:%j:%H:%M:%S')
            else:
                dataset.acq_time = datetime.strptime(content_list[29][:-8], '%Y:%j:%H:%M:%S')
            dataset.level = content_list[53]

            container = dict()
            container['quicklook'] = content_list[5]
            sensor = dataset.sensor
            if sensor == "OLI_TIRS":
                sensor = "LANDSAT_8"
            container['metadata'] = USGS_HTTP_SERVICE % (
                int(content_list[6]), int(content_list[6]), int(content_list[7]), int(content_list[7]), sensor,
                dataset.acq_time.strftime("%Y-%m-%d"), dataset.acq_time.strftime("%Y-%m-%d"))

            usgs = USGSOrderContainer()
            usgs.link = content_list[54]
            container.update(usgs.to_dict())

            google = GoogleLandsatContainer()
            google_link = GoogleLandsatContainer.base % (
                GoogleLandsatContainer.supported_sensors[content_list[1]], int(content_list[6]), int(content_list[7]), content_list[0])
            if remote_file_exists(google_link):
                google.link = google_link
                container.update(google.to_dict())

            dataset.resources = container

            datasets.append(dataset)

        if (counter % 25000) == 0:
            print counter

    return datasets


def import_from_file(in_csv):
    datasets = landsat_harvester(in_csv)

    api = Api()
    skipped = list()
    registered = list()

    for c, ds in enumerate(datasets):
        try:
            out = api.create_dataset(ds)

            if not 'title' in str(out):
                registered.append(c)
            else:
                skipped.append(c)
        except Exception, e:
            print e
        if c % 100 == 0:
            print c
            print 'skipped:', skipped
            print 'registered:', registered
            skipped = list()
            registered = list()


def import_from_pipe(lines):
    datasets = landsat_harvester_line(lines)
    api = Api()
    skipped = list()
    registered = list()

    for c, ds in enumerate(datasets):
        try:
            out = api.create_dataset(ds)
            if not 'already' in str(out):
                registered.append(c)
            else:
                skipped.append(c)
        except Exception, e:
            print e
    print 'registered:', registered
    print 'skipped:', skipped
