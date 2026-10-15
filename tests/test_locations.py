import unittest
from pathlib import Path

from collector.collector import load_locations


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class LocationCatalogueTests(unittest.TestCase):
    def test_osm_location_catalogue_has_unique_valid_rows(self):
        locations = load_locations(PROJECT_ROOT / "config" / "traffic_locations.csv")
        self.assertEqual(len(locations), 16)
        self.assertEqual(len({row["location_id"] for row in locations}), 16)
        self.assertEqual(len({row["osm_way_id"] for row in locations}), 16)
        self.assertTrue(all(row["osm_relation_id"] > 0 for row in locations))
        self.assertTrue(all(33.45 <= row["latitude"] <= 33.70 for row in locations))
        self.assertTrue(all(-7.75 <= row["longitude"] <= -7.45 for row in locations))
        self.assertTrue(all(row["road_name_osm"].strip() for row in locations))

    def test_all_sixteen_arrondissements_are_represented_once(self):
        locations = load_locations(PROJECT_ROOT / "config" / "traffic_locations.csv")
        districts = [row["arrondissement"] for row in locations]
        self.assertEqual(len(set(districts)), 16)


if __name__ == "__main__":
    unittest.main()
