import json
import logging
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ASRANK_ENDPOINT = "https://api.asrank.caida.org/v2/graphql"


def ts_to_date_str(ts):
    """
    Convert timestamp to a date. This is used for ASRank API which only takes
    date strings with no time as parameters.
    """
    return datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d")


class AsRank:
    """
    Utilities for using ASRank services
    """

    def __init__(self, max_ts=""):
        self.data_ts = None

        # various caches to avoid duplicate queries
        self.cache = None
        self.cone_cache = None
        self.neighbors_cache = None
        self.siblings_cache = None
        self.organization_cache = None

        self.queries_sent = 0

        self.session = None
        self._initialize_session()

        self.init_cache(max_ts)

    def _initialize_session(self):
        self.session = requests.Session()
        retries = Retry(total=5,
                        backoff_factor=1,
                        status_forcelist=[500, 502, 503, 504])
        self.session.mount(ASRANK_ENDPOINT, HTTPAdapter(max_retries=retries))

    def _close_session(self):
        if self.session:
            self.session.close()

    def _send_request(self, query):
        """
        send requests to ASRank endpoint
        :param query:
        :return:
        """

        r = self.session.post(url=ASRANK_ENDPOINT, json={'query': query})
        r.raise_for_status()
        self.queries_sent += 1
        return r

    def init_cache(self, ts):
        """
        Initialize the ASRank cache for the timestamp ts
        :param ts:
        :return:
        """
        self.cache = {}
        self.cone_cache = {}
        self.neighbors_cache = {}
        self.siblings_cache = {}
        self.organization_cache = {}
        self.queries_sent = 0
        if isinstance(ts, int):
            ts = ts_to_date_str(ts)

        ####
        # Try to cache datasets available before the given ts
        ####
        graphql_query = """
            {
              datasets(dateStart:"2000-01-01", dateEnd:"%s", sort:"-date", first:1){
                edges {
                  node {
                    date
                  }
                }
              }
            }
        """ % ts
        r = self._send_request(graphql_query)

        edges = r.json()['data']['datasets']['edges']
        if edges:
            self.data_ts = edges[0]["node"]["date"]
            return

        # if code reaches here, we have not found any datasets before ts. we should now try to find one after ts.
        # this is the best effort results
        logging.warning("cannot find dataset before date %s, looking for the closest one after it now" % ts)

        graphql_query = """
            {
              datasets(dateStart:"%s", sort:"date", first:1){
                edges {
                  node {
                    date
                  }
                }
              }
            }
        """ % ts
        r = self._send_request(graphql_query)
        edges = r.json()['data']['datasets']['edges']
        if edges:
            self.data_ts = edges[0]["node"]["date"]
            logging.warning("found closest dataset date to be %s" % self.data_ts)
            return
        else:
            raise ValueError("no datasets from ASRank available to use for tagging")

    def _query_asrank_for_asns(self, asns, chunk_size=100):
        asns = [str(asn) for asn in asns]
        asns_needed = [asn for asn in asns if asn not in self.cache]
        if not asns_needed:
            return

        # https://stackoverflow.com/a/312464/768793
        def chunks(lst, n):
            """Yield successive n-sized chunks from lst."""
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        for asns in chunks(asns_needed, chunk_size):

            graphql_query = """
                {
                  asns(asns: %s, dateStart: "%s", dateEnd: "%s", first:%d, sort:"-date") {
                    edges {
                      node {
                        date
                        asn
                        asnName
                        rank
                        organization{
                          country{
                            iso
                            name
                          }
                          orgName
                          orgId
                        } asnDegree {
                          provider
                          peer
                          customer
                          total
                          transit
                          sibling
                        }
                      }
                    }
                  }
                }
            """ % (json.dumps(asns), self.data_ts, self.data_ts, len(asns))
            r = self._send_request(graphql_query)
            try:
                for node in r.json()['data']['asns']['edges']:
                    data = node['node']
                    if data['asn'] not in self.cache:
                        if "asnDegree" in data:
                            degree = data["asnDegree"]
                            degree["provider"] = degree["provider"] or 0
                            degree["customer"] = degree["customer"] or 0
                            degree["peer"] = degree["peer"] or 0
                            degree["sibling"] = degree["sibling"] or 0
                            data["asnDegree"] = degree
                        self.cache[data['asn']] = data
                for asn in asns:
                    if asn not in self.cache:
                        self.cache[asn] = None
            except KeyError as e:
                logging.error("Error in node: {}".format(r.json()))
                logging.error("Request: {}".format(graphql_query))
                raise e

    ##########
    # AS_ORG #
    ##########

    def are_siblings(self, asn1, asn2):
        """
        Check if two ASes are sibling ASes, i.e. belonging to the same organization
        :param asn1: first asn
        :param asn2: second asn
        :return: True if asn1 and asn2 belongs to the same organization
        """
        self._query_asrank_for_asns([asn1, asn2])
        if any([self.cache[asn] is None for asn in [asn1, asn2]]):
            return False
        try:
            return self.cache[asn1]["organization"]["orgId"] == self.cache[asn2]["organization"]["orgId"]
        except TypeError:
            # we have None for some of the values
            return False

    def get_organization(self, asn):
        """
        Keys:
        - country
        - orgName
        - orgId

        Example return value:
        {'country': {'iso': 'US', 'name': 'United States'}, 'orgName': 'Google LLC', 'orgId': 'f7b8c6de69'}

        :param asn:
        :return:
        """
        self._query_asrank_for_asns([asn])
        if self.cache[asn] is None:
            return None
        return self.cache[asn]["organization"]

    def get_registered_country(self, asn):
        """
        Get ASes registered country, formated in ISO country code. For example: United States -> US.
        """
        self._query_asrank_for_asns([asn])
        if self.cache[asn] is None:
            return None

        try:
            return self.cache[asn]["organization"]["country"]["iso"]
        except KeyError:
            return None
        except TypeError:
            return None

    ###########
    # AS_RANK #
    ###########

    def get_degree(self, asn):
        """
        Get relationship summary for asn, including number of customers, providers, peers, etc.

        Example return dictionary:
        {
            "provider": 0,
            "peer": 31,
            "customer": 1355,
            "total": 1386,
            "transit": 1318,
            "sibling": 25
        }
        :param asn:
        :return:
        """
        self._query_asrank_for_asns([asn])
        if self.cache[asn] is None:
            return None

        return self.cache[asn]["asnDegree"]

    def is_sole_provider(self, asn_pro, asn_cust):
        """
        Verifies if asn_pro and asn_cust are in a customer provider relationship
        and asn_pro is the sole upstream of asn_cust (no other providers nor peers
        are available to asn_cust).

        This function is ported from dataconcierge.ASRank.check_single_upstream. The name of which is confusing, thus
        renamed to is_sole_provider.

        :param asn_pro: provider ASn (string)
        :param asn_cust: ASn in customer cone (string)
        :return: True or False
        """
        asn_cust_degree = self.get_degree(asn_cust)
        if asn_cust_degree is None:
            # missing data for asn_cust
            return False
        if asn_cust_degree["provider"] == 1 and asn_cust_degree["peer"] == 0 and \
                self.get_relationship(asn_pro, asn_cust) == "p-c":
            # asn_cust has one provider, no peer, and the provider is asn_pro
            return True
        return False

    def get_relationship(self, asn0, asn1):
        """
        Get the AS relationship between asn0 and asn1.

        asn0 is asn1's:
        - provider: "p-c"
        - customer: "c-p"
        - peer: "p-p"
        - other: None

        :param asn0:
        :param asn1:
        :return:
        """
        graphql_query = """
            {
              asnLink(asn0:"%s", asn1:"%s", date:"%s"){
              relationship
              }
            }
        """ % (asn0, asn1, self.data_ts)
        r = self._send_request(graphql_query)
        if r.json()["data"]["asnLink"] is None:
            return None
        rel = r.json()["data"]["asnLink"].get("relationship", "")

        if rel == "provider":
            # asn1 is the provider of asn0
            return "c-p"

        if rel == "customer":
            # asn1 is the customer of asn0
            return "p-c"

        if rel == "peer":
            # asn1 is the peer of asn0
            return "p-p"

        return None

    def in_customer_cone(self, asn0, asn1):
        """
        Check if asn0 is in the customer cone of asn1
        :param asn0:
        :param asn1:
        :return:
        """
        if asn1 in self.cone_cache:
            return asn0 in self.cone_cache[asn1]

        graphql_query = """
        {
          asnCone(asn:"%s", date:"%s"){
            asns {
              edges {
                node {
                  asn
                }
              }
            }
          }
        }
        """ % (asn1, self.data_ts)
        r = self._send_request(graphql_query)
        data = r.json()["data"]["asnCone"]
        if data is None:
            return False
        asns_in_cone = {node["node"]["asn"] for node in data["asns"]["edges"]}
        self.cone_cache[asn1] = asns_in_cone
        return asn0 in asns_in_cone

    def cache_asrank_chunk(self, asns: list, chunk_size: int):
        """
        Query asrank info in chunk to boost individual asrank queries performance later.

        :param asns:
        :param chunk_size:
        :return:
        """
        self._query_asrank_for_asns(asns, chunk_size)

    def get_all_siblings_list(self, asns, chunk_size=100):
        self._query_asrank_for_asns(asns, chunk_size)
        res = {}
        for asn in asns:
            res[asn] = self.get_all_siblings(asn)
        return res

    def get_all_siblings(self, asn, skip_asrank_call=False):
        """
        get all siblings for an ASN
        :param asn: AS number to query for all siblings
        :param skip_asrank_call: skip asrank call if already done
        :return: a tuple of (TOTAL_COUNT, ASNs)
        """
        # FIXME: pagination does not work here. Example ASN5313.
        asn = str(asn)
        if asn in self.siblings_cache:
            return self.siblings_cache[asn]

        if not skip_asrank_call:
            self._query_asrank_for_asns([asn])

        if asn not in self.cache or self.cache[asn] is None:
            return 0, []
        asrank_info = self.cache[asn]
        if "organization" not in asrank_info or asrank_info["organization"] is None:
            return 0, []

        org_id = self.cache[asn]["organization"]["orgId"]

        if org_id in self.organization_cache:
            data = self.organization_cache[org_id]
        else:
            graphql_query = """
            {
            organization(orgId:"%s"){
              orgId,
              orgName,
              members{
                numberAsns,
                numberAsnsSeen,
                asns{totalCount,edges{node{asn,asnName}}}
              }
            }}        
            """ % org_id
            r = self._send_request(graphql_query)
            data = r.json()["data"]["organization"]
            self.organization_cache[org_id] = data

        if data is None:
            return 0, []

        total_cnt = data["members"]["asns"]["totalCount"]
        siblings = set()
        for sibling_data in data["members"]["asns"]["edges"]:
            siblings.add(sibling_data["node"]["asn"])
        if asn in siblings:
            siblings.remove(asn)
            total_cnt -= 1

        # NOTE: this assert can be wrong when number of siblings needs pagination
        # assert len(siblings) == total_cnt - 1

        siblings = list(siblings)
        self.neighbors_cache[asn] = (total_cnt, siblings)
        return total_cnt, siblings

    def get_neighbor_ases(self, asn):
        if asn in self.neighbors_cache:
            return self.neighbors_cache[asn]

        res = {"providers": [], "customers": [], "peers": []}
        graphql_query = """
        {
          asn(asn: "%s") {
            asn
            asnLinks {
              edges {
                node {
                  asn1 {
                    asn
                  }
                  relationship
                }
              }
            }
          }
        }
        """ % asn
        r = self._send_request(graphql_query)

        data = r.json()["data"]["asn"]
        if data is None:
            return res
        for neighbor in data["asnLinks"]["edges"]:
            neighbor_asn = neighbor["node"]["asn1"]["asn"]
            neighbor_rel = neighbor["node"]["relationship"]
            res["{}s".format(neighbor_rel)].append(neighbor_asn)
        self.neighbors_cache[asn] = res
        return res

    def get_asrank_for_asns(self, asn_lst):
        """
        retrieve ASRank data for asns.
        :param asn_lst:
        :return:
        """

        asn_lst = [str(asn) for asn in asn_lst]
        self._query_asrank_for_asns(asn_lst)

        res = {}
        for asn in asn_lst:
            res[asn] = self.cache.get(asn, None)
        return res

