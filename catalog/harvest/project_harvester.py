# Created by sgebhardt at 22.08.16
# Copyright EOSS GmbH 2016
from manage.landsat_catalog import USGSCatalog
from manage.planetlabscatalog import PlanetCatalog
from manage.sentinelcatalog import SentinelCatalog
from model import Context
from model.orm import EossProject

from osgeo import ogr


class ProjectHarvester(object):
    def __init__(self, projectname):
        self.datasets = set()
        self.session = Context().getSession()
        self.project = self.session.query(EossProject).filter(EossProject.name == projectname).first()
        self.geom = self.session.scalar(self.project.geom.ST_AsText())  # Shall we apply a standard buffer?

    def find(self):

        """
        @param project: EossProject object
        """
        geom = ogr.CreateGeometryFromWkt(self.geom)
        x_min, x_max, y_min, y_max = geom.GetEnvelope()
        aoi_nw = (x_min, y_max)
        aoi_se = (x_max, y_min)
        aoi_ne = (aoi_se[0], aoi_nw[1])
        aoi_sw = (aoi_nw[0], aoi_se[1])
        aoi = [aoi_nw, aoi_ne, aoi_se, aoi_sw, aoi_nw]

        cat = PlanetCatalog()
        result = cat.find('planetscope', aoi, self.project.project_start, self.project.project_end)
        for r in result:
            self.datasets.add(r)

        cat = PlanetCatalog()
        result = cat.find('rapideye', aoi, self.project.project_start, self.project.project_end)
        for r in result:
            self.datasets.add(r)

        cat = SentinelCatalog()
        result = cat.find('sentinel2', aoi, self.project.project_start, self.project.project_end)
        for r in result:
            self.datasets.add(r)

        cat = USGSCatalog()
        result = cat.find('LANDSAT_8', aoi, self.project.project_start, self.project.project_end,
                          1.0)  # and we filter for cloudiness before downloading and standardisation
        for r in result:
            self.datasets.add(r)

    def staging(self):
        for d in self.datasets:
            self.session.add(d)
        self.session.commit()


if __name__ == '__main__':
    ph = ProjectHarvester('montesazules_mx')
    ph.find()
    ph.staging()
    print len(ph.datasets)
