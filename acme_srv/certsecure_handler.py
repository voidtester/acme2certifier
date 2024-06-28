# -*- coding: utf-8 -*-
""" Skeleton for customized CA handler """
from __future__ import print_function
from typing import Tuple
import requests
from acme_srv.helper import load_config, header_info_get
import time
from configparser import ConfigParser

#conf=ConfigParser.read('EAB')
class CAhandler(object):
    """ EST CA handler """

    def __init__(self, _debug: bool = None, logger: object = None):
        self.logger = logger
        self.parameter = None
        self.csmurl = "https://192.168.29.7"

    def __enter__(self):
        """ Makes CAhandler a Context Manager """
        if not self.parameter:
            self._config_load()
        return self
        
    
    def __exit__(self, exc_type, exc_value, traceback):
        """ Close the connection at the end of the context """
        pass

    def _config_load(self):
        """ Load config from file """
        self.logger.debug('CAhandler._config_load()')

        config_dic = load_config(self.logger, 'CAhandler')
        if 'CAhandler' in config_dic and 'parameter' in config_dic['CAhandler']:
            self.parameter = config_dic['CAhandler']['parameter']

        self.logger.debug('CAhandler._config_load() ended')

    def _stub_func(self, parameter: str):
        """ Stub function """
        self.logger.debug('CAhandler._stub_func(%s)', parameter)
        self.logger.debug('CAhandler._stub_func() ended')

    def get_cert(self, cert_taskID,kid5):
        try:
            url = f'https://192.168.29.7/acme/get_cert'
            json_payload = {
                'taskID': cert_taskID,
                'kid5':kid5
                }
            response = requests.post(url, json=json_payload, verify=False)
            print(response.text)
            if response.status_code == 200:
                response_json = response.json()
                return response_json.get('cert')
            else:
                return None
        except Exception as e:
            print('Error in getting cert:', e)
            return None

    def get_kid(self):
        
        return "ODQzMzg5ZWZhMzI5NDNhZmE5YTQ5MTM1YTYzNDJmYjM3ZGYzNDQ4ZGM0ZTk0YzQ1YTdjMjQ5ZmRlYzJjMDIyZQ=="

    def get_cert_details(self, kid):
        self.logger.debug('Get Cert details initiated')

        url = self.csmurl + '/acme/get_ca_details'
        params = {'kid5': kid}

        try:
            res = requests.get(url=url, params=params,verify=False)
            res.raise_for_status()  
        except requests.RequestException as e:
            self.logger.error(f"Error fetching certificate details: {e}")
            return None, None

        self.logger.debug('Get Cert details ended')
        #print(res.text, "response from api get cert details")

        try:
            data = res.json()  
            ca = data['CA info']['ca']
            templ = data['CA info']['template']
            #print(ca, templ)
            return ca, templ
        except (ValueError, KeyError) as e:
            self.logger.error(f"Error parsing response JSON: {e}")
            return None, None

    def enroll(self, csr: str) -> Tuple[str, str, str, str]:
        """ Enroll certificate """
        self.logger.debug('CAhandler.enroll()')

        cert_bundle = None
        error = None
        cert_raw = None
        poll_identifier = None

        kid = self.get_kid()
        print(kid, "kid found")
        ca, template = self.get_cert_details(kid)
        print("ca found",ca)
                        
        if csr:
            csr_pem = f"-----BEGIN CERTIFICATE REQUEST-----\n{csr}\n-----END CERTIFICATE REQUEST-----"
    
            # Add the BEGIN and END headers and footers
            print(csr_pem)
            url = self.csmurl + '/acme/cert_request'
            json_payload = {'csr': csr_pem, 'ca': ca, 'template': template,'owner':'infinitevoid@duck.com','kid5':kid}

            response = requests.post(url, json=json_payload, verify=False)
            if response:
                print(response.text)
                taskid=str(response['TaskID'])
            
            
            get_cert=self.csmurl+"/acme/get_cert"
            json_payload={'taskID':taskid}
            flag=True
            while flag:
                resp=requests.post(url=get_cert,json=json_payload,verify=False)
                if resp.status_code == 200:
                    response_json = resp.json()
                    cert_raw=response_json.get('cert')
                    flag=False
                
                time.sleep(10)    
                
        # Optional: lookup HTTP header information from request
        qset = header_info_get(self.logger, csr=csr)
        if qset:
            self.logger.info(qset[-1]['header_info'])

        #self._stub_func(csr)
        self.logger.debug('CAhandler.enroll() ended')

        return (error, cert_bundle, cert_raw, poll_identifier)

    def poll(self, cert_name: str, poll_identifier: str, _csr: str) -> Tuple[str, str, str, str, bool]:
        """ Poll status of pending CSR and download certificates """
        self.logger.debug('CAhandler.poll()')

        error = None
        cert_bundle = None
        cert_raw = None
        rejected = False

        self._stub_func(cert_name)
        self.logger.debug('CAhandler.poll() ended')

        return (error, cert_bundle, cert_raw, poll_identifier, rejected)

    def revoke(self, _cert: str, _rev_reason: str, _rev_date: str) -> Tuple[int, str, str]:
        """ Revoke certificate """
        self.logger.debug('CAhandler.revoke()')

        code = 500
        message = 'urn:ietf:params:acme:error:serverInternal'
        detail = 'Revocation is not supported.'

        self.logger.debug('CAhandler.revoke() ended')
        return (code, message, detail)

    def trigger(self, payload: str) -> Tuple[str, str, str]:
        """ Process trigger message and return certificate """
        self.logger.debug('CAhandler.trigger()')

        error = None
        cert_bundle = None
        cert_raw = None

        self._stub_func(payload)
        self.logger.debug('CAhandler.trigger() ended with error: %s', error)

        return (error, cert_bundle, cert_raw)
