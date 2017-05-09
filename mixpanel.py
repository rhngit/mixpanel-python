import hashlib
import time
import urllib
import urllib2
import base64
import json


class Mixpanel(object):

    def __init__(self, api_key, api_secret, token):
        self.api_key = api_key
        self.api_secret = api_secret
        self.token = token
        self.dataExportUrl = 'http://mixpanel.com/api/2.0/'
        self.bulkExportUrl = 'http://data.mixpanel.com/api/2.0/'
        self.setUserUrl = 'http://api.mixpanel.com/engage/'

    def request(self, params, endpoint='engage'):
        """Set up the MP request"""

        params['api_key'] = self.api_key
        params['expire'] = int(time.time())+600  # 600 is ten minutes from now
        if 'sig' in params:
            del params['sig']
        params['sig'] = self.hash_args(params)

        request_url = self.dataExportUrl + endpoint + '/?' + self.unicode_urlencode(params)

        request = urllib.urlopen(request_url)
        data = request.read()

        return data

    def get_paged_results(self, params, endpoint='engage'):
        response = self.request(params, endpoint)

        params['session_id'] = json.loads(response)['session_id']
        params['page'] = 0
        global_total = json.loads(response)['total']

        rawdata = []

        if global_total > 0:
            print "Users: %d" % global_total
            has_results = True
            total = 0
            while has_results:
                responser = json.loads(response)['results']
                total += len(responser)
                has_results = len(responser) == 1000
                for data in responser:
                    rawdata.append(json.dumps(data))
                print "Got %d/%d" % (total, global_total)
                params['page'] += 1
                if has_results:
                    response = self.request(params, endpoint)

        return rawdata

    def hash_args(self, args, secret=None):
        """Hash dem arguments in the proper way
        join keys - values and append a secret -> md5 it"""

        for a in args:
            if isinstance(args[a], list):
                args[a] = json.dumps(args[a])

        args_joined = ''
        for a in sorted(args.keys()):
            if isinstance(a, unicode):
                args_joined += a.encode('utf-8')
            else:
                args_joined += str(a)

            args_joined += "="

            if isinstance(args[a], unicode):
                args_joined += args[a].encode('utf-8')
            else:
                args_joined += str(args[a])

        args_hashed = hashlib.md5(args_joined)

        if secret:
            args_hashed.update(secret)
        elif self.api_secret:
            args_hashed.update(self.api_secret)
        return args_hashed.hexdigest()

    def unicode_urlencode(self, params):
        """Convert stuff to json format and correctly handle unicode url parameters"""

        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params[i] = (param[0], json.dumps(param[1]),)

        result = urllib.urlencode([(k, isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params])
        return result

    def send_user_info(self, data):
        payload = {"data": base64.b64encode(json.dumps(data)),
                   "verbose": 1,
                   "api_key": self.api_key}

        response = urllib2.urlopen(self.setUserUrl, urllib.urlencode(payload))
        message = response.read()

        '''if something goes wrong, this will say what'''
        if json.loads(message)['status'] != 1:
            print message

    def set_properties(self, userlist):
        batch = []
        for distinct_id, property, value in userlist:
            params = {'token': self.token,
                      '$distinct_id': distinct_id,
                      '$set': {property: value},
                      '$ignore_time': 'true',
                      '$ip': 0}

            batch.append(params)

        self.send_user_info(batch)

    def batch_set_properties(self, users):
        log_chunk = 500
        counter = len(users) // log_chunk
        while len(users):
            batch = users[:50]

            self.set_properties(batch)
            if len(users) // log_chunk != counter:
                counter = len(users) // log_chunk
                print "%d users left" % len(users)
            users = users[50:]

    def unset_property(self, userlist, key):
        batch = []
        for user in userlist:
            # print user
            params = {'token': self.token,
                      '$distinct_id': user,
                      '$unset': [key],
                      '$ignore_time': 'true',
                      '$ip': 0}

            batch.append(params)
        self.send_user_info(batch)

    def batch_unset_property(self, users, key):
        counter = len(users) // 500
        while len(users):
            batch = users[:50]
            # print batch
            self.unset_property(batch, key)
            if len(users) // 500 != counter:
                counter = len(users) // 500
                print "%d users left!" % len(users)
            users = users[50:]
