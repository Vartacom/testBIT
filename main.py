import requests
import time
import pandas as pd
import config

cookies = {
    'bnc-uuid': 'a32d58b8-81f5-4070-b64e-058595f43897',
    'changeBasisTimeZone': '',
    'campaign': 'www.google.com',
    'BNC_FV_KEY': '33c555703495adfe848aa8e5bf49cbb7cf8bda08',
    'BNC_FV_KEY_EXPIRE': '1684257643080',
    'userPreferredCurrency': 'RUB_USD',
    'sajssdk_2015_cross_new_user': '1',
    'fiat-prefer-currency': 'EUR',
    'OptanonAlertBoxClosed': '2023-05-16T11:20:47.769Z',
    'g_state': '{"i_p":1684243390323,"i_l":1}',
    'futures-layout': 'pro',
    '_ga': 'GA1.2.879571644.1684236421',
    '_gid': 'GA1.2.1773712127.1684236421',
    'sensorsdata2015jssdkcross': '%7B%22distinct_id%22%3A%22188244a84b4eba-070a1d139855f74-26031a51-1183200-188244a84b5ff3%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E8%87%AA%E7%84%B6%E6%90%9C%E7%B4%A2%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC%22%2C%22%24latest_referrer%22%3A%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24latest_utm_source%22%3A%22fan-token%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTg4MjQ0YTg0YjRlYmEtMDcwYTFkMTM5ODU1Zjc0LTI2MDMxYTUxLTExODMyMDAtMTg4MjQ0YTg0YjVmZjMifQ%3D%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%22188244a84b4eba-070a1d139855f74-26031a51-1183200-188244a84b5ff3%22%7D',
    'fan-token-init-compliance': 'null',
    'source': 'fan-token',
    'OptanonConsent': 'isGpcEnabled=0&datestamp=Tue+May+16+2023+15%3A18%3A05+GMT%2B0300+(%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0%2C+%D1%81%D1%82%D0%B0%D0%BD%D0%B4%D0%B0%D1%80%D1%82%D0%BD%D0%BE%D0%B5+%D0%B2%D1%80%D0%B5%D0%BC%D1%8F)&version=202211.1.0&isIABGlobal=false&hosts=&consentId=283bb1c9-a701-4269-8655-f3c8e6537323&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A0%2CC0004%3A0%2CC0002%3A0&geolocation=RU%3BMOS&AwaitingReconsent=false',
    'lang': 'ru',
}

headers = {
    'authority': 'www.binance.com',
    'accept': '*/*',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'bnc-uuid': 'a32d58b8-81f5-4070-b64e-058595f43897',
    'clienttype': 'web',
    'content-type': 'application/json',
    # 'cookie': 'bnc-uuid=a32d58b8-81f5-4070-b64e-058595f43897; changeBasisTimeZone=; campaign=www.google.com; BNC_FV_KEY=33c555703495adfe848aa8e5bf49cbb7cf8bda08; BNC_FV_KEY_EXPIRE=1684257643080; userPreferredCurrency=RUB_USD; sajssdk_2015_cross_new_user=1; fiat-prefer-currency=EUR; OptanonAlertBoxClosed=2023-05-16T11:20:47.769Z; g_state={"i_p":1684243390323,"i_l":1}; futures-layout=pro; _ga=GA1.2.879571644.1684236421; _gid=GA1.2.1773712127.1684236421; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22188244a84b4eba-070a1d139855f74-26031a51-1183200-188244a84b5ff3%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E8%87%AA%E7%84%B6%E6%90%9C%E7%B4%A2%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC%22%2C%22%24latest_referrer%22%3A%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24latest_utm_source%22%3A%22fan-token%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTg4MjQ0YTg0YjRlYmEtMDcwYTFkMTM5ODU1Zjc0LTI2MDMxYTUxLTExODMyMDAtMTg4MjQ0YTg0YjVmZjMifQ%3D%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%22188244a84b4eba-070a1d139855f74-26031a51-1183200-188244a84b5ff3%22%7D; fan-token-init-compliance=null; source=fan-token; OptanonConsent=isGpcEnabled=0&datestamp=Tue+May+16+2023+15%3A18%3A05+GMT%2B0300+(%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0%2C+%D1%81%D1%82%D0%B0%D0%BD%D0%B4%D0%B0%D1%80%D1%82%D0%BD%D0%BE%D0%B5+%D0%B2%D1%80%D0%B5%D0%BC%D1%8F)&version=202211.1.0&isIABGlobal=false&hosts=&consentId=283bb1c9-a701-4269-8655-f3c8e6537323&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A0%2CC0004%3A0%2CC0002%3A0&geolocation=RU%3BMOS&AwaitingReconsent=false; lang=ru',
    'csrftoken': 'd41d8cd98f00b204e9800998ecf8427e',
    'device-info': 'eyJzY3JlZW5fcmVzb2x1dGlvbiI6IjgxNiwxNDUwIiwiYXZhaWxhYmxlX3NjcmVlbl9yZXNvbHV0aW9uIjoiNzc2LDE0NTAiLCJzeXN0ZW1fdmVyc2lvbiI6IldpbmRvd3MgMTAiLCJicmFuZF9tb2RlbCI6InVua25vd24iLCJzeXN0ZW1fbGFuZyI6InJ1LVJVIiwidGltZXpvbmUiOiJHTVQrMyIsInRpbWV6b25lT2Zmc2V0IjotMTgwLCJ1c2VyX2FnZW50IjoiTW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzExMy4wLjAuMCBTYWZhcmkvNTM3LjM2IiwibGlzdF9wbHVnaW4iOiJQREYgVmlld2VyLENocm9tZSBQREYgVmlld2VyLENocm9taXVtIFBERiBWaWV3ZXIsTWljcm9zb2Z0IEVkZ2UgUERGIFZpZXdlcixXZWJLaXQgYnVpbHQtaW4gUERGIiwiY2FudmFzX2NvZGUiOiJhOWEzZjNjNyIsIndlYmdsX3ZlbmRvciI6Ikdvb2dsZSBJbmMuIChJbnRlbCkiLCJ3ZWJnbF9yZW5kZXJlciI6IkFOR0xFIChJbnRlbCwgSW50ZWwoUikgVUhEIEdyYXBoaWNzIDYyMCBEaXJlY3QzRDExIHZzXzVfMCBwc181XzAsIEQzRDExKSIsImF1ZGlvIjoiMTI0LjA0MzQ3NTI3NTE2MDc0IiwicGxhdGZvcm0iOiJXaW4zMiIsIndlYl90aW1lem9uZSI6IkV1cm9wZS9Nb3Njb3ciLCJkZXZpY2VfbmFtZSI6IkNocm9tZSBWMTEzLjAuMC4wIChXaW5kb3dzKSIsImZpbmdlcnByaW50IjoiM2RlOGRiMDA3Y2U4MDllMWUzMDQ1NzE4YmYwMmVlZjUiLCJkZXZpY2VfaWQiOiIiLCJyZWxhdGVkX2RldmljZV9pZHMiOiIifQ==',
    'fvideo-id': '33c555703495adfe848aa8e5bf49cbb7cf8bda08',
    'lang': 'ru',
    'referer': 'https://www.binance.com/ru/trade/ETH_USDT?theme=dark&type=spot',
    'sec-ch-ua': '"Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
    'x-passthrough-token': '',
    'x-trace-id': '7e9c23d1-9fb2-482b-887a-af1e6298e556',
    'x-ui-request-trace': '7e9c23d1-9fb2-482b-887a-af1e6298e556',
}

params = {
    'limit': '80',
    'symbol': 'ETHUSDT',
}

connection = config.create_obw()
sql_insert = 'INSERT OR IGNORE INTO cripto (id, price, data) VALUES (?,?,?);'
while True:
    response = requests.get('https://www.binance.com/api/v1/aggTrades', params=params, cookies=cookies, headers=headers).json()
    for one_res in response:
        # print(one_res['a'], one_res['p'], one_res['T'])
        bd_inst = one_res['a'], one_res['p'], one_res['T']
        config.execute_query(connection, sql_insert, bd_inst)

    time_h = (time.time() * 1000) - 3600000
    df = pd.read_sql_query(F'select * from cripto', connection)
    df_time = df[df.data >= ((time.time() * 1000) - 3600000)]
    config.execute_query1(connection, f'DELETE FROM cripto WHERE data <= {time_h}')
    max_pr = df_time.price.max()
    min_pr = df_time.price.min()
    print(max_pr, min_pr)
    procent = pr = 100.0 * (df_time.price.max() - df_time.price.min()) / df_time.price.min()
    print(procent)
    if procent >= 1:
        print(df_time.price.head(1))
        print('OOOOOOOOp')
        config.execute_query1(connection, f'DELETE FROM cripto')
    time.sleep(4)
    print('-----------------------------')