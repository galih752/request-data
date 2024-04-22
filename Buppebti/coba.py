import requests

cookies = {
    '_ga': 'GA1.3.1250708061.1709710870',
    '_gid': 'GA1.3.426870307.1709710870',
    '_gat_boardTracker': '1',
    '_ga_1GGWF9M52W': 'GS1.3.1709710869.1.1.1709711170.0.0.0',
}

headers = {
    'authority': 'ideas.veo.co',
    'accept': '*/*',
    'accept-language': 'id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7',
    'authorization': 'null',
    'content-type': 'application/json',
    # 'cookie': '_ga=GA1.3.1250708061.1709710870; _gid=GA1.3.426870307.1709710870; _gat_boardTracker=1; _ga_1GGWF9M52W=GS1.3.1709710869.1.1.1709711170.0.0.0',
    'origin': 'https://ideas.veo.co',
    'referer': 'https://ideas.veo.co/16',
    'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
}

json_data = [
    {
        'operationName': 'CommentLikesTooltip',
        'variables': {
            'commentId': 'b31b4d9c-601d-49ab-9193-e66017a3cf9a',
        },
        'query': 'query CommentLikesTooltip($commentId: String!) {\n  comment(id: $commentId) {\n    id\n    likers {\n      id\n      isAnonymous\n      name\n      __typename\n    }\n    __typename\n  }\n}\n',
    },
]

response = requests.post('https://ideas.veo.co/graphql', cookies=cookies, headers=headers, json=json_data)

print(response.json())