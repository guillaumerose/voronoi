import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.triangulate.VoronoiDiagramBuilder;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.MalformedURLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class App {

    public static class Local {
        Point point;
        long id;

        Local(Point point, long id) {
            this.point = point;
            this.id = id;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(new Date());


        GeometryFactory gf = new GeometryFactory();
        STRtree tree = new STRtree();
        Set<Coordinate> seen = new HashSet<>();

        int j = 0;

        DataStore dataStore = DataStoreFinder.getDataStore(dataStoreDetails());
        FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore.getFeatureSource(dataStore.getTypeNames()[0]);
        try (FeatureIterator<SimpleFeature> features = source.getFeatures(Filter.INCLUDE).features()) {
            while (features.hasNext()) {
                SimpleFeature feature = features.next();
                org.locationtech.jts.geom.Point value = (org.locationtech.jts.geom.Point) feature.getDefaultGeometryProperty().getValue();
                Point point = gf.createPoint(new Coordinate(value.getX(), value.getY()));
                if (!seen.contains(new Coordinate(value.getX(), value.getY()))) {
                    seen.add(new Coordinate(value.getX(), value.getY()));
                    tree.insert(point.getEnvelopeInternal(), new Local(point, (long) feature.getAttribute("ID")));
                }
                if (j % 100000 == 0) {
                    System.out.println(j);
                }
                j++;
            }
        }
        dataStore.dispose();


        j = 0;

        BufferedWriter writer = new BufferedWriter(new FileWriter("/tmp/output.csv"));

        dataStore = DataStoreFinder.getDataStore(dataStoreDetails());
        source = dataStore.getFeatureSource(dataStore.getTypeNames()[0]);
        try (FeatureIterator<SimpleFeature> features = source.getFeatures(Filter.INCLUDE).features()) {
            while (features.hasNext()) {
                SimpleFeature feature = features.next();
                org.locationtech.jts.geom.Point value = (org.locationtech.jts.geom.Point) feature.getDefaultGeometryProperty().getValue();
                Point point = gf.createPoint(new Coordinate(value.getX(), value.getY()));
                List<Local> neighbors = find(tree, point.buffer(1500)).collect(Collectors.toList());

                if (neighbors.size() == 1) {
                    writer.write(feature.getAttribute("ID") + ";" + feature.getAttribute("BHD2020") + ";" + point.buffer(500) + "\n");
                } else {
                    Point[] points = new Point[neighbors.size()];
                    for (int i = 0; i < neighbors.size(); i++) {
                        points[i] = neighbors.get(i).point;
                    }
                    MultiPoint multiPoint = gf.createMultiPoint(points);
                    VoronoiDiagramBuilder builder = new VoronoiDiagramBuilder();
                    builder.setSites(multiPoint);
                    Geometry voronoi = builder.getDiagram(gf);

                    for (int i = 0; i < voronoi.getNumGeometries(); i++) {
                        Geometry polygon = voronoi.getGeometryN(i);
                        if (polygon.contains(point)) {
                            writer.write(feature.getAttribute("ID") + ";" + feature.getAttribute("BHD2020") + ";" + polygon.intersection(point.buffer(500)) + "\n");
                        }
                    }
                }
                if (j % 1000 == 0) {
                    System.out.println(j);
                }
                j++;
                if (j == 100000) {
                    break;
                }
            }
        }

        writer.close();
        dataStore.dispose();
    }

    private static Map<String, Object> dataStoreDetails() throws MalformedURLException {
        File file = new File("/Users/guillaumerose/Ariane/shp_simu2020_cohesion_super_france/simu_2020_cohesion_super_france.shp");
        Map<String, Object> map = new HashMap<>();
        map.put("url", file.toURI().toURL());
        return map;
    }

    private static Stream<Local> find(STRtree tree, Geometry polygon) {
        List<Local> query = tree.query(polygon.getEnvelopeInternal());
        return query.stream().filter(p -> polygon.contains(p.point));
    }
}
