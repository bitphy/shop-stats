"""
    Unitary Testing for the shop_raw_df
"""
import requests
import numpy as np
import pandas as pd
from shopstats import response_is_ok, get_shops, \
        get_nodepoint_entries, generate_dataframe

# Mocking utilities
class MockResponse:


    def __init__(self, status_code=200, headers=None, text=''):
        self.status_code = status_code
        self.headers = headers if headers else { 'content-type': 'application/json; charset=utf-8' }
        self.text = text

def build_mock_request(contents):
    """ given a list of contents (str) to be return by a mocked request
        it returns a function that will deliver each entry of the contents """

    def generator(contents):
        for c in contents:
            yield c
    gen = generator(contents)
    def mock_request(*args, **kwargs):
        nonlocal gen
        return MockResponse(text=next(gen))
    return mock_request


def test_response_is_ok_when_status_code_300():
    response = MockResponse(status_code=300)
    assert not response_is_ok(response)


def test_response_is_ok_when_status_code_ok_but_content_type_not_json():
    response = MockResponse(headers = { 'content-type': 'non json related' })
    assert not response_is_ok(response)


def test_response_is_ok_when_everything_is_ok():
    response = MockResponse()
    assert response_is_ok(response)


def test_get_shops_when_none(monkeypatch):
    contents = '[{ "id": "anychain", "shops": [] }]'


    def mock_request(*args, **kwargs):
        return MockResponse(text=contents)
    monkeypatch.setattr(requests, 'request', mock_request)
    expected = []
    found = get_shops()
    assert expected == found


def test_get_shops_when_one(monkeypatch):
    contents = '[{ "id": "chain_1", "shops": [ {"id": "shop_id_1", "name":"shop_name_1"} ] }]'


    def mock_request(*args, **kwargs):
        return MockResponse(text=contents)
    monkeypatch.setattr(requests, 'request', mock_request)
    expected = [ ('chain_1', 'shop_id_1', 'shop_name_1') ]
    found = get_shops()
    assert expected == found


def test_get_shops_when_some(monkeypatch):
    contents = '''[{ "id": "chain_1", "shops": [
                        {"id": "shop_id_1", "name":"shop_name_1"},
                        {"id": "shop_id_2", "name":"shop_name_2"},
                        {"id": "shop_id_3", "name":"shop_name_3"}
                     ] }]'''


    def mock_request(*args, **kwargs):
        return MockResponse(text=contents)
    monkeypatch.setattr(requests, 'request', mock_request)
    expected = [ ('chain_1', 'shop_id_1', 'shop_name_1'),
                 ('chain_1', 'shop_id_2', 'shop_name_2'),
                 ('chain_1', 'shop_id_3', 'shop_name_3') ]
    found = get_shops()
    assert expected == found


def test_get_shops_when_two_chains(monkeypatch):
    contents = '''[{ "id": "chain_1", "shops": [
                        {"id": "shop_id_1", "name":"shop_name_1"},
                        {"id": "shop_id_2", "name":"shop_name_2"}]},
                   { "id": "chain_2", "shops": [
                        {"id": "shop_id_3", "name":"shop_name_3"}
                     ] }]'''


    def mock_request(*args, **kwargs):
        return MockResponse(text=contents)
    monkeypatch.setattr(requests, 'request', mock_request)
    expected = [ ('chain_1', 'shop_id_1', 'shop_name_1'),
                 ('chain_1', 'shop_id_2', 'shop_name_2'),
                 ('chain_2', 'shop_id_3', 'shop_name_3') ]
    found = get_shops()
    assert expected == found



def test_generate_dataframe_when_response_error(monkeypatch):
    def mock_request(*args, **kwargs):
        return MockResponse(status_code=300)

    monkeypatch.setattr(requests, 'request', mock_request)
    nodepoint_specs = [
            { "name": "products", "type": "raw", "column_suffix": "distinct", "equality_key": "originalId" },
            { "name": "sellers", "type": "raw", "column_suffix": "distinct", "equality_key": "originalId" },
        ]
    expected = pd.DataFrame(columns=[
        'chain_id', 'shop_id', 'shop_name',
        'products_count', 'products_distinct', 'products_malformed',
        'sellers_count', 'sellers_distinct', 'sellers_malformed',
        ])
    found = generate_dataframe(nodepoint_specs)
    pd.testing.assert_frame_equal(expected, found, check_dtype=False)

def test_generate_dataframe_when_no_chain(monkeypatch):
    contents = '[]'

    def mock_request(*args, **kwargs):
        return MockResponse(text=contents)

    nodepoint_specs = [
            { "name": "products", "type": "raw", "column_suffix": "distinct", "equality_key": "originalId" },
            { "name": "sellers", "type": "raw", "column_suffix": "distinct", "equality_key": "originalId" },
        ]

    monkeypatch.setattr(requests, 'request', mock_request)
    expected = pd.DataFrame(columns=[
        'chain_id', 'shop_id', 'shop_name',
        'products_count', 'products_distinct', 'products_malformed',
        'sellers_count', 'sellers_distinct', 'sellers_malformed',
        ])
    found = generate_dataframe(nodepoint_specs)
    pd.testing.assert_frame_equal(expected, found, check_dtype=False)


def test_generate_dataframe_when_badformed_response(monkeypatch):
    contents = '[{}]'

    def mock_request(*args, **kwargs):
        return MockResponse(text=contents)

    monkeypatch.setattr(requests, 'request', mock_request)
    nodepoint_specs = [
            { "name": "products", "type": "raw", "column_suffix": "distinct", "equality_key": "originalId" },
            { "name": "sellers", "type": "raw", "column_suffix": "distinct", "equality_key": "originalId" },
        ]
    expected = pd.DataFrame(columns=[
        'chain_id', 'shop_id', 'shop_name',
        'products_count', 'products_distinct', 'products_malformed',
        'sellers_count', 'sellers_distinct', 'sellers_malformed',
        ])
    found = generate_dataframe(nodepoint_specs)
    pd.testing.assert_frame_equal(expected, found, check_dtype=False)


def test_generate_dataframe_when_no_shops(monkeypatch):
    content_list = [
            '[{ "id": "anychain", "shops": [] }]'
            ]
    num_content = 0     # next content to deliver by a request
    nodepoint_specs = [
            { "name": "products", "type": "raw", "column_suffix": "distinct", "equality_key": "originalId" },
            { "name": "sellers", "type": "raw", "column_suffix": "distinct", "equality_key": "originalId" },
        ]


    def mock_request(*args, **kwargs):
        nonlocal num_content
        response = MockResponse(text=content_list[num_content])
        num_content += 1
        return response

    monkeypatch.setattr(requests, 'request', mock_request)
    expected = pd.DataFrame(columns=[
        'chain_id', 'shop_id', 'shop_name',
        'products_count', 'products_distinct', 'products_malformed',
        'sellers_count', 'sellers_distinct', 'sellers_malformed',
        ])
    found = generate_dataframe(nodepoint_specs)
    pd.testing.assert_frame_equal(expected, found, check_dtype=False)



def test_generate_dataframe_when_one_shop_one_nodepoint_with_gen(monkeypatch):
    content_list = [
            '[{ "id": "chain_1", "shops": [ {"id": "shop_id_1", "name":"shop_name_1"} ] }]',
            '''[
                    { "originalId": "oid1", "name":"name1" },
                    { "originalId": "oid1", "name":"name1" },
                    { "id": "badentry" },
                    { "originalId": "oid2", "name":"name2" }
                ]'''
            ]
    nodepoint_specs = [ { "name": "sellers", "type": "raw", "column_suffix": "distinct", "equality_key": "originalId" }, ]

    mock_request = build_mock_request(content_list)
    monkeypatch.setattr(requests, 'request', mock_request)

    expected = pd.DataFrame([['chain_1', 'shop_id_1', 'shop_name_1', 3, 2, 1]],
            columns = ['chain_id', 'shop_id', 'shop_name',
                'sellers_count', 'sellers_distinct', 'sellers_malformed', ])

    found = generate_dataframe(nodepoint_specs)
    pd.testing.assert_frame_equal(expected, found, check_dtype=False)


def test_generate_dataframe_when_one_shop_one_nodepoint(monkeypatch):
    content_list = [
            '[{ "id": "chain_1", "shops": [ {"id": "shop_id_1", "name":"shop_name_1"} ] }]',
            '''[
                    { "originalId": "oid1", "name":"name1" },
                    { "originalId": "oid1", "name":"name1" },
                    { "id": "badentry" },
                    { "originalId": "oid2", "name":"name2" }
                ]'''
            ]
    num_content = 0     # next content to deliver by a request

    nodepoint_specs = [ { "name": "sellers", "type": "raw", "column_suffix": "distinct", "equality_key": "originalId" }, ]


    def mock_request(*args, **kwargs):
        nonlocal num_content
        response = MockResponse(text=content_list[num_content])
        num_content += 1
        return response

    monkeypatch.setattr(requests, 'request', mock_request)
    expected = pd.DataFrame([['chain_1', 'shop_id_1', 'shop_name_1', 3, 2, 1]],
            columns = ['chain_id', 'shop_id', 'shop_name',
                'sellers_count', 'sellers_distinct', 'sellers_malformed', ])

    found = generate_dataframe(nodepoint_specs)
    pd.testing.assert_frame_equal(expected, found, check_dtype=False)


def test_generate_dataframe_when_many_shop_many_nodepoint(monkeypatch):
    content_list = [
            '''[ { "id": "chain_1", "shops": [
                    {"id": "shop_id_1", "name":"shop_name_1"},
                    {"id": "shop_id_2", "name":"shop_name_2"}
                    ] }]''',
            '''[
                    { "whatis": "product_1s", "originalId": "oid1", "name":"name1" },
                    { "whatis": "product_1s", "originalId": "oid1", "name":"name1" },
                    { "whatis": "product_1s", "originalId": "oid2", "name":"name2" }
                ]''',
            '''[
                    { "whatis": "sellers_1", "originalId": "oid1", "name":"name1" },
                    { "whatis": "sellers_1", "originalId": "oid1", "name":"name1" },
                    { "whatis": "sellers_1", "id": "badentry" },
                    { "whatis": "sellers_1", "originalId": "oid2", "name":"name2" }
                ]''',
            '''[
                    { "whatis": "product_2s", "originalId": "oid1", "name":"name1" }
                ]''',
            '''[
                    { "whatis": "sellers_2", "originalId": "oid1", "name":"name1" },
                    { "whatis": "sellers_2", "originalId": "oid2", "name":"name2" }
                ]''',
            ]
    num_content = 0     # next content to deliver by a request
    nodepoint_specs = [
            { "name": "products", "type": "raw", "column_suffix": "distinct", "equality_key": "originalId" },
            { "name": "sellers", "type": "raw", "column_suffix": "distinct", "equality_key": "originalId" },
        ]


    def mock_request(*args, **kwargs):
        nonlocal num_content
        response = MockResponse(text=content_list[num_content])
        num_content += 1
        return response

    monkeypatch.setattr(requests, 'request', mock_request)
    expected = pd.DataFrame([
        ['chain_1', 'shop_id_1', 'shop_name_1', 3, 2, 0, 3, 2, 1],
        ['chain_1', 'shop_id_2', 'shop_name_2', 1, 1, 0, 2, 2, 0],
        ],
        columns = ['chain_id', 'shop_id', 'shop_name',
            'products_count', 'products_distinct', 'products_malformed',
            'sellers_count', 'sellers_distinct', 'sellers_malformed',
            ])

    found = generate_dataframe(nodepoint_specs)
    pd.testing.assert_frame_equal(expected, found, check_dtype=False)


def test_generate_dataframe_when_aggregation_nodepoint_one_entry_no_subkey(monkeypatch):
    content_list = [
            '''[{ "id": "chain_1", "shops": [
                        {"id": "shop_1", "name": "shopname_1"}
               ] }]''',
            '''[
                    { "billing": 111 }
                ]'''
            ]
    num_content = 0     # next content to deliver by a request

    def mock_request(*args, **kwargs):
        nonlocal num_content
        response = MockResponse(text=content_list[num_content])
        num_content += 1
        return response

    nodepoint_specs = [
            { 'name': 'test', 'type': 'aggregation', 'aggregation_key': 'billing', 'column_suffix': 'billing', 'subkey': None }
            ]

    monkeypatch.setattr(requests, 'request', mock_request)

    expected = pd.DataFrame([
        ['chain_1', 'shop_1', 'shopname_1', 1, 111, 0],
        ],
        columns = [ 'chain_id', 'shop_id', 'shop_name',
            'test_count', 'test_billing', 'test_malformed' ])
    found = generate_dataframe(nodepoint_specs)
    pd.testing.assert_frame_equal(expected, found, check_dtype=False)



def test_generate_dataframe_when_aggregation_nodepoint_many_entries_no_subkey(monkeypatch):
    content_list = [
            '''[{ "id": "chain_1", "shops": [
                        {"id": "shop_1", "name": "shopname_1"}
               ] }]''',
            '''[
                    { "billing": 111 },
                    { "billing": 222 }
                ]'''
            ]
    num_content = 0     # next content to deliver by a request

    def mock_request(*args, **kwargs):
        nonlocal num_content
        response = MockResponse(text=content_list[num_content])
        num_content += 1
        return response

    nodepoint_specs = [
            { 'name': 'test', 'type': 'aggregation', 'aggregation_key': 'billing', 'column_suffix': 'billing', 'subkey': None }
            ]

    monkeypatch.setattr(requests, 'request', mock_request)

    expected = pd.DataFrame([
        ['chain_1', 'shop_1', 'shopname_1', 2, 333, 0],
        ],
        columns = [ 'chain_id', 'shop_id', 'shop_name',
            'test_count', 'test_billing', 'test_malformed' ])
    found = generate_dataframe(nodepoint_specs)
    pd.testing.assert_frame_equal(expected, found, check_dtype=False)

def test_generate_dataframe_when_aggregation_nodepoint_one_entry_with_subkey(monkeypatch):
    content_list = [
            '''[{ "id": "chain_1", "shops": [
                        {"id": "shop_1", "name": "shopname_1"}
               ] }]''',
            '''[
                    { "sales": [{ "billing": 111 }] }
                ]'''
            ]
    num_content = 0     # next content to deliver by a request

    def mock_request(*args, **kwargs):
        nonlocal num_content
        response = MockResponse(text=content_list[num_content])
        num_content += 1
        return response

    nodepoint_specs = [
            { 'name': 'test', 'type': 'aggregation', 'aggregation_key': 'billing', 'column_suffix': 'billing', 'subkey': 'sales' }
            ]

    monkeypatch.setattr(requests, 'request', mock_request)

    expected = pd.DataFrame([
        ['chain_1', 'shop_1', 'shopname_1', 1, 111, 0],
        ],
        columns = [ 'chain_id', 'shop_id', 'shop_name',
            'test_count', 'test_billing', 'test_malformed' ])
    found = generate_dataframe(nodepoint_specs)
    pd.testing.assert_frame_equal(expected, found, check_dtype=False)


def test_generate_dataframe_when_aggregation_nodepoint_with_many_entries_same_subkey(monkeypatch):
    content_list = [
            '''[{ "id": "chain_1", "shops": [
                        {"id": "shop_1", "name": "shopname_1"}
               ] }]''',
            '''[
                    { "sales": [
                        { "billing": 111 },
                        { "billing": 222 },
                        { "billing": 333 }
                    ] }
                ]'''
            ]
    num_content = 0     # next content to deliver by a request

    def mock_request(*args, **kwargs):
        nonlocal num_content
        response = MockResponse(text=content_list[num_content])
        num_content += 1
        return response

    nodepoint_specs = [
            { 'name': 'test', 'type': 'aggregation', 'aggregation_key': 'billing', 'column_suffix': 'billing', 'subkey': 'sales' }
            ]

    monkeypatch.setattr(requests, 'request', mock_request)

    expected = pd.DataFrame([
        ['chain_1', 'shop_1', 'shopname_1', 3, 666, 0],
        ],
        columns = [ 'chain_id', 'shop_id', 'shop_name',
            'test_count', 'test_billing', 'test_malformed' ])
    found = generate_dataframe(nodepoint_specs)
    pd.testing.assert_frame_equal(expected, found, check_dtype=False)

def test_generate_dataframe_when_aggregation_nodepoint_with_many_entries_different_subkey(monkeypatch):
    content_list = [
            '''[{ "id": "chain_1", "shops": [
                        {"id": "shop_1", "name": "shopname_1"}
               ] }]''',
            '''[
                    { "sales": [
                        { "billing": 1 },
                        { "billing": 2 },
                        { "billing": 3 }
                    ] },
                    { "sales": [
                        { "billing": 10 },
                        { "billing": 20 },
                        { "billing": 30 }
                    ] },
                    { "sales": [
                        { "billing": 100 },
                        { "billing": 200 },
                        { "billing": 300 }
                    ] }
                ]'''
            ]
    num_content = 0     # next content to deliver by a request

    def mock_request(*args, **kwargs):
        nonlocal num_content
        response = MockResponse(text=content_list[num_content])
        num_content += 1
        return response

    nodepoint_specs = [
            { 'name': 'test', 'type': 'aggregation', 'aggregation_key': 'billing', 'column_suffix': 'billing', 'subkey': 'sales' }
            ]

    monkeypatch.setattr(requests, 'request', mock_request)

    expected = pd.DataFrame([
        ['chain_1', 'shop_1', 'shopname_1', 9, 666, 0],
        ],
        columns = [ 'chain_id', 'shop_id', 'shop_name',
            'test_count', 'test_billing', 'test_malformed' ])
    found = generate_dataframe(nodepoint_specs)
    pd.testing.assert_frame_equal(expected, found, check_dtype=False)

def test_generate_dataframe_when_aggregation_nodepoint_with_malformed_entry_no_subkey(monkeypatch):
    content_list = [
            '''[{ "id": "chain_1", "shops": [
                        {"id": "shop_1", "name": "shopname_1"}
               ] }]''',
            '''[
                    { "not the expected subkey": [
                        { "billing": 1 },
                        { "billing": 2 },
                        { "billing": 3 }
                    ] },
                    { "sales": [
                        { "billing": 10 },
                        { "billing": 20 },
                        { "billing": 30 }
                    ] },
                    { "sales": [
                        { "billing": 100 },
                        { "billing": 200 },
                        { "billing": 300 }
                    ] }
                ]'''
            ]
    num_content = 0     # next content to deliver by a request

    def mock_request(*args, **kwargs):
        nonlocal num_content
        response = MockResponse(text=content_list[num_content])
        num_content += 1
        return response

    nodepoint_specs = [
            { 'name': 'test', 'type': 'aggregation', 'aggregation_key': 'billing', 'column_suffix': 'billing', 'subkey': 'sales' }
            ]

    monkeypatch.setattr(requests, 'request', mock_request)

    expected = pd.DataFrame([
        ['chain_1', 'shop_1', 'shopname_1', 6, 660, 1],
        ],
        columns = [ 'chain_id', 'shop_id', 'shop_name',
            'test_count', 'test_billing', 'test_malformed' ])
    found = generate_dataframe(nodepoint_specs)
    pd.testing.assert_frame_equal(expected, found, check_dtype=False)


def test_generate_dataframe_when_aggregation_nodepoint_with_malformed_entry_no_aggregation_key(monkeypatch):
    content_list = [
            '''[{ "id": "chain_1", "shops": [
                        {"id": "shop_1", "name": "shopname_1"}
               ] }]''',
            '''[
                    { "sales": [
                        { "not the expected key": 1 },
                        { "billing": 2 },
                        { "billing": 3 }
                    ] },
                    { "sales": [
                        { "billing": 10 },
                        { "not the expected key": 20 },
                        { "billing": 30 }
                    ] },
                    { "sales": [
                        { "billing": 100 },
                        { "billing": 200 },
                        { "not the expected key": 300 }
                    ] }
                ]'''
            ]
    num_content = 0     # next content to deliver by a request

    def mock_request(*args, **kwargs):
        nonlocal num_content
        response = MockResponse(text=content_list[num_content])
        num_content += 1
        return response

    nodepoint_specs = [
            { 'name': 'test', 'type': 'aggregation', 'aggregation_key': 'billing', 'column_suffix': 'billing', 'subkey': 'sales' }
            ]

    monkeypatch.setattr(requests, 'request', mock_request)

    expected = pd.DataFrame([
        ['chain_1', 'shop_1', 'shopname_1', 6, 345, 3],
        ],
        columns = [ 'chain_id', 'shop_id', 'shop_name',
            'test_count', 'test_billing', 'test_malformed' ])
    found = generate_dataframe(nodepoint_specs)
    pd.testing.assert_frame_equal(expected, found, check_dtype=False)


def test_generate_dataframe_when_aggregation_mani_nodepoints_same_shop(monkeypatch):
    content_list = [
            '''[{ "id": "chain_1", "shops": [
                        {"id": "shop_1", "name": "shopname_1"}
               ] }]''',
            '''[
                    { "sales": [
                        { "billing": 1 }
                    ] },
                    { "sales": [
                        { "billing": 10 }
                    ] },
                    { "sales": [
                        { "billing": 100 }
                    ] }
                ]''',
            '''[
                    { "sales": [
                        { "billing": 2 }
                    ] },
                    { "sales": [
                        { "billing": 200 }
                    ] }
                ]''',
            '''[
                    { "billing": 111 },
                    { "billing": 222 },
                    { "non expected": 555 }
                ]'''

            ]
    num_content = 0     # next content to deliver by a request

    def mock_request(*args, **kwargs):
        nonlocal num_content
        response = MockResponse(text=content_list[num_content])
        num_content += 1
        return response

    nodepoint_specs = [
            { 'name': 'test_1', 'type': 'aggregation', 'aggregation_key': 'billing', 'column_suffix': 'billing', 'subkey': 'sales' },
            { 'name': 'test_2', 'type': 'aggregation', 'aggregation_key': 'billing', 'column_suffix': 'billing', 'subkey': 'sales' },
            { 'name': 'test_3', 'type': 'aggregation', 'aggregation_key': 'billing', 'column_suffix': 'billing', 'subkey': None }
            ]

    monkeypatch.setattr(requests, 'request', mock_request)

    expected = pd.DataFrame([
        ['chain_1', 'shop_1', 'shopname_1', 3, 111, 0, 2, 202, 0, 2, 333, 1],
        ],
        columns = [ 'chain_id', 'shop_id', 'shop_name',
            'test_1_count', 'test_1_billing', 'test_1_malformed',
            'test_2_count', 'test_2_billing', 'test_2_malformed',
            'test_3_count', 'test_3_billing', 'test_3_malformed'
            ])
    found = generate_dataframe(nodepoint_specs)
    pd.testing.assert_frame_equal(expected, found, check_dtype=False)





def test_generate_dataframe_when_aggregation_many_nodepoints_many_shops_and_chains(monkeypatch):
    content_list = [
            '''[{ "id": "chain_1", "shops": [
                        {"id": "shop_1", "name": "shopname_1"},
                        {"id": "shop_2", "name": "shopname_2"}
               ] },
                { "id": "chain_2", "shops": [
                        {"id": "shop_3", "name": "shopname_3"}
                ]}
               ]''',
            '''[
                    { "whatis": "for chain_1 shop_1 test_1", "sales": [
                        { "billing": 1 }
                    ] },
                    { "whatis": "for chain_1 shop_1 test_1", "sales": [
                        { "billing": 10 }
                    ] },
                    { "whatis": "for chain_1 shop_1 test_1", "sales": [
                        { "billing": 100 }
                    ] }
                ]''',
            '''[
                    { "whatis": "for chain_1 shop_1 test_2","sales": [
                        { "billing": 2 }
                    ] },
                    { "whatis": "for chain_1 shop_1 test_2","sales": [
                        { "billing": 200 }
                    ] }
                ]''',
            '''[
                    { "whatis": "for chain_1 shop_1 test_3","billing": 111 },
                    { "whatis": "for chain_1 shop_1 test_3","billing": 222 },
                    { "whatis": "for chain_1 shop_1 test_3","non expected": 555 }
                ]''',
            '''[
                    { "whatis": "for chain_1 shop_2 test_1", "sales": [
                        { "billing": 1000 }
                        ] }
                ]''',
            '''[
                    { "whatis": "for chain_1 shop_2 test_2", "sales": [
                        { "billing": 2000 }
                        ] }
                ]''',
            '''[
                    { "whatis": "for chain_1 shop_2 test_3", 
                      "billing": 3000 } 
                ]''',
            '''[
                    { "whatis": "for chain_2 shop_3 test_1", "sales": [
                        { "billing": 10000 }
                        ] }
                ]''',
            '''[
                    { "whatis": "for chain_2 shop_3 test_2", "sales": [
                        { "billing": 20000 }
                        ] }
                ]''',
            '''[
                    { "whatis": "for chain_2 shop_3 test_3", 
                      "billing": 30000 }
                ]''',


            ]
    num_content = 0     # next content to deliver by a request

    def mock_request(*args, **kwargs):
        nonlocal num_content
        response = MockResponse(text=content_list[num_content])
        num_content += 1
        return response

    nodepoint_specs = [
            { 'name': 'test_1', 'type': 'aggregation', 'aggregation_key': 'billing', 'column_suffix': 'billing', 'subkey': 'sales' },
            { 'name': 'test_2', 'type': 'aggregation', 'aggregation_key': 'billing', 'column_suffix': 'billing', 'subkey': 'sales' },
            { 'name': 'test_3', 'type': 'aggregation', 'aggregation_key': 'billing', 'column_suffix': 'billing', 'subkey': None }
            ]

    monkeypatch.setattr(requests, 'request', mock_request)

    expected = pd.DataFrame([
        ['chain_1', 'shop_1', 'shopname_1', 3, 111, 0, 2, 202, 0, 2, 333, 1],
        ['chain_1', 'shop_2', 'shopname_2', 1, 1000, 0, 1, 2000, 0, 1, 3000, 0],
        ['chain_2', 'shop_3', 'shopname_3', 1, 10000, 0, 1, 20000, 0, 1, 30000, 0]
        ],
        columns = [ 'chain_id', 'shop_id', 'shop_name',
            'test_1_count', 'test_1_billing', 'test_1_malformed',
            'test_2_count', 'test_2_billing', 'test_2_malformed',
            'test_3_count', 'test_3_billing', 'test_3_malformed'
            ])
    found = generate_dataframe(nodepoint_specs)
    pd.testing.assert_frame_equal(expected, found, check_dtype=False)



def test_generate_dataframe_when_raw_and_aggregation_many_nodepoints_many_shops_and_chains(monkeypatch):
    content_list = [
            '''[{ "id": "chain_1", "shops": [
                        {"id": "shop_1", "name": "shopname_1"},
                        {"id": "shop_2", "name": "shopname_2"}
               ] },
                { "id": "chain_2", "shops": [
                        {"id": "shop_3", "name": "shopname_3"}
                ]}
               ]''',
            '''[
                    { "whatis": "for chain_1 shop_1 test_1", "sales": [
                        { "billing": 1 }
                    ] },
                    { "whatis": "for chain_1 shop_1 test_1", "sales": [
                        { "billing": 10 }
                    ] },
                    { "whatis": "for chain_1 shop_1 test_1", "sales": [
                        { "billing": 100 }
                    ] }
                ]''',
            '''[
                    { "whatis": "for chain_1 shop_1 test_2", "originalId": "oid1"},
                    { "whatis": "for chain_1 shop_1 test_2", "originalId": "oid2"},
                    { "whatis": "for chain_1 shop_1 test_2", "originalId": "oid2"},
                    { "whatis": "for chain_1 shop_1 test_2", "unexpected key": "oid3"}
                ]''',
            '''[
                    { "whatis": "for chain_1 shop_1 test_3","billing": 111 },
                    { "whatis": "for chain_1 shop_1 test_3","billing": 222 },
                    { "whatis": "for chain_1 shop_1 test_3","non expected": 555 }
                ]''',
            '''[
                    { "whatis": "for chain_1 shop_2 test_1", "sales": [
                        { "billing": 1000 }
                        ] }
                ]''',
            '''[
                    { "whatis": "for chain_1 shop_2 test_2", "originalId": "oid122"}
                ]''',
            '''[
                    { "whatis": "for chain_1 shop_2 test_3", 
                      "billing": 3000 } 
                ]''',
            '''[
                    { "whatis": "for chain_2 shop_3 test_1", "sales": [
                        { "billing": 10000 }
                        ] }
                ]''',
            '''[
                    { "whatis": "for chain_1 shop_3 test_2", "originalId": "oid232"}
                ]''',
            '''[
                    { "whatis": "for chain_2 shop_3 test_3", 
                      "billing": 30000 }
                ]''',


            ]
    num_content = 0     # next content to deliver by a request

    def mock_request(*args, **kwargs):
        nonlocal num_content
        response = MockResponse(text=content_list[num_content])
        num_content += 1
        return response

    nodepoint_specs = [
            { 'name': 'test_1', 'type': 'aggregation', 'aggregation_key': 'billing', 'column_suffix': 'billing', 'subkey': 'sales' },
            { 'name': 'test_2', 'type': 'raw', 'equality_key': 'originalId', 'column_suffix': 'distinct' },
            { 'name': 'test_3', 'type': 'aggregation', 'aggregation_key': 'billing', 'column_suffix': 'billing', 'subkey': None }
            ]

    monkeypatch.setattr(requests, 'request', mock_request)

    expected = pd.DataFrame([
        ['chain_1', 'shop_1', 'shopname_1', 3, 111, 0,   3, 2, 1, 2, 333, 1],
        ['chain_1', 'shop_2', 'shopname_2', 1, 1000, 0,  1, 1, 0, 1, 3000, 0],
        ['chain_2', 'shop_3', 'shopname_3', 1, 10000, 0, 1, 1, 0, 1, 30000, 0]
        ],
        columns = [ 'chain_id', 'shop_id', 'shop_name',
            'test_1_count', 'test_1_billing', 'test_1_malformed',
            'test_2_count', 'test_2_distinct', 'test_2_malformed',
            'test_3_count', 'test_3_billing', 'test_3_malformed'
            ])
    found = generate_dataframe(nodepoint_specs)
    pd.testing.assert_frame_equal(expected, found, check_dtype=False)











