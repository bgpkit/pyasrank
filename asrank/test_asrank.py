import unittest

from asrank import AsRank


class TestAsRank(unittest.TestCase):
    """
    Tests for inference engine logic
    """

    def setUp(self):
        """
        Initialize an inference engine before each test function.
        """
        self.asrank = AsRank(max_ts="2020-07-02")

    def tearDown(self):
        self.asrank._close_session()

    def test_data_date(self):
        """
        Test looking for asrank most recent available dataset date.
        """
        self.assertEqual(self.asrank.data_ts, "2020-07-01")
        self.assertEqual(self.asrank.queries_sent, 1)

    def test_asorg_siblings(self):
        """
        Test if two ASes are siblings.
        """
        self.assertTrue(self.asrank.are_siblings("701", "702"))
        self.assertFalse(self.asrank.are_siblings("701", "15169"))
        self.assertEqual(self.asrank.queries_sent, 3)

    def test_asorg_country(self):
        """
        Test getting AS registered countries.
        """
        self.assertEqual(self.asrank.get_registered_country("701"), "US")
        self.assertEqual(self.asrank.get_registered_country("1111701"), None)
        self.assertEqual(self.asrank.queries_sent, 3)

    def test_asrank_degree(self):
        """
        Test getting degree of ASNs.
        """
        # existing ASN
        self.assertEqual(self.asrank.get_degree("701"), {
            "provider": 0,
            "peer": 33,
            "customer": 1376,
            "total": 1409,
            "transit": 1358,
            "sibling": 22
        })
        # non-existing ASN
        self.assertEqual(self.asrank.get_degree("1111701"), None)
        self.assertEqual(self.asrank.queries_sent, 3)

    def test_asrank_rel(self):
        """
        Test getting relationships between ASes.
        """
        self.assertEqual(self.asrank.get_relationship("15169", "36040"), "p-c")
        self.assertEqual(self.asrank.get_relationship("36040", "15169"), "c-p")

        self.assertEqual(self.asrank.get_relationship("3356", "3"), "p-c")
        self.assertEqual(self.asrank.get_relationship("3", "3356"), "c-p")

        self.assertEqual(self.asrank.get_relationship("36416", "3933"), "p-c")
        self.assertEqual(self.asrank.get_relationship("3933", "36416"), "c-p")

        self.assertEqual(self.asrank.get_relationship("15169", "11136040"), None)

    def test_asrank_in_cone(self):
        """
        Test if any two ASes are within cone of each other
        """
        self.assertTrue(self.asrank.in_customer_cone("36040", "36040"))  # AS itself should be in it's cone
        self.assertTrue(self.asrank.in_customer_cone("36040", "15169"))
        self.assertTrue(self.asrank.in_customer_cone("43515", "15169"))
        self.assertFalse(self.asrank.in_customer_cone("15169", "36040"))
        self.assertFalse(self.asrank.in_customer_cone("15169", "111136040"))

    def test_asrank_is_sole_provider(self):
        """
        Check if an AS is the sole provider of another AS
        """
        self.assertTrue(self.asrank.is_sole_provider("12008", "397231"))  # Single provider
        self.assertFalse(self.asrank.is_sole_provider("3701", "3582"))  # One of two providers
        self.assertFalse(self.asrank.is_sole_provider("15169", "3582"))  # Not provider

    def test_asrank_get_neighbors(self):
        self.assertEqual({"providers": [], "customers": [], "peers": []}, self.asrank.get_neighbor_ases("131565"))

    def test_asrank_get_all_siblings(self):
        total, siblings = self.asrank.get_all_siblings("3356")
        self.assertEqual(34, total)
        total, siblings = self.asrank.get_all_siblings("15169")
        self.assertEqual(8, total)

    def test_preload_asrank(self):
        asns = ["524", "10753", 3356]
        self.asrank.cache_asrank_chunk(asns, 100)
        for asn in asns:
            self.asrank.get_all_siblings(asn, skip_asrank_call=True)


    def test_asrank_get_all_siblings_list(self):
        data_map = self.asrank.get_all_siblings_list(["3356", "15169"])

        self.assertEqual(34, data_map["3356"][0])
        self.assertEqual(8, data_map["15169"][0])
