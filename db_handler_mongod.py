from pymongo import MongoClient
from acme_srv.helper import datestr_to_date, load_config
import json
from typing import List, Tuple, Dict
from acme_srv.version import __dbversion__

def initialize():
    """ run db_handler specific initialization functions  """
    # pylint: disable=W0107
    pass


def dict_from_row(row):
        """ small helper to convert the output of a "select" command into a dictionary """
        return dict(zip(row.keys(), row))

class DBstore:
    def __init__(self, debug: bool = False, logger: object = None, db_name: str = None):
        self.debug = debug
        self.logger = logger

        # Connect to the MongoDB server
        self.client = MongoClient()
        # Choose a database, use db_name if provided or a default
        self.db = self.client[db_name or 'default_db']

        # Define collections
        self.account_collection = self.db['account']
        self.authorization_collection = self.db['authorization']
        self.orders_collection = self.db['orders']
        self.status_collection = self.db['status']
    
    
    
    def _account_search(self, column: str, string: str) -> Dict:
        """ Search the account collection for a certain key/value pair. """
        self.logger.debug('DBStore._account_search(column:%s, pattern:%s)', column, string)
        
        # MongoDB uses find_one to retrieve a single document
        try:
            result = self.account_collection.find_one({column: {'$regex': string, '$options': 'i'}})
        except Exception as err:
            self.logger.error('DBStore._account_search(column:%s, pattern:%s) failed with err: %s', column, string, err)
            result = None

        self.logger.debug('DBStore._account_search() ended with: %s', bool(result))
        return result

    def _authorization_search(self, column: str, string: str) -> List[Dict]:
        """Search the authorization collection for a certain key/value pair."""
        self.logger.debug('DBStore._authorization_search(column:%s, pattern:%s)', column, string)

        try:
            # Construct the aggregation pipeline
            pipeline = [
                {
                    '$lookup': {
                        'from': 'orders',
                        'localField': 'order',
                        'foreignField': '_id',
                        'as': 'order_data'
                    }
                },
                {
                    '$unwind': {
                        'path': '$order_data',
                        'preserveNullAndEmptyArrays': True
                    }
                },
                {
                    '$lookup': {
                        'from': 'status',
                        'localField': 'status',
                        'foreignField': '_id',
                        'as': 'status_data'
                    }
                },
                {
                    '$unwind': {
                        'path': '$status_data',
                        'preserveNullAndEmptyArrays': True
                    }
                },
                {
                    '$lookup': {
                        'from': 'account',
                        'localField': 'order_data.account',
                        'foreignField': '_id',
                        'as': 'account_data'
                    }
                },
                {
                    '$unwind': {
                        'path': '$account_data',
                        'preserveNullAndEmptyArrays': True
                    }
                },
                {
                    '$match': {
                        column: {'$regex': f'.*{string}.*', '$options': 'i'}
                    }
                },
                {
                '$project': {
                    '_id': 0,  # Exclude _id field from the result
                    'type': 1,
                    'value': 1,
                    'name': {'$ifNull': ['$authorization.name', '']},  # Handle potential absence of authorization.name
                    'order__id': '$order_data._id',
                    'order__name': '$order_data.name',
                    'status_id': '$status_data._id',
                    'status__name': '$status_data.name',
                    'order__account__name': '$account_data.name'  # Access name directly from account_data
                }
            }
            ]

            # Execute the aggregation pipeline
            result = list(self.authorization_collection.aggregate(pipeline))

        except Exception as err:
            self.logger.error('DBStore._authorization_search(column:%s, pattern:%s) failed with err: %s', column, string, err)
            result = []

        self.logger.debug('DBStore._authorization_search() ended')
        return result
    
    def _cahandler_search(self, column: str, string: str) -> Dict:
        """ Search the cahandler collection for a certain key/value pair. """
        self.logger.debug('DBStore._cahandler_search(column:%s, pattern:%s)', column, string)

        try:
            # Perform the search using MongoDB find_one with a regex pattern
            result = self.db['cahandler'].find_one({column: {'$regex': string, '$options': 'i'}})
        except Exception as err:
            self.logger.error('DBStore._cahandler_search(column:%s, pattern:%s) failed with err: %s', column, string, err)
            result = None

        self.logger.debug('DBStore._cahandler_search() ended')
        return result
    
    def _certificate_account_check(self, account_name: str, certificate_dic: Dict[str, str], order_dic: Dict[str, str]) -> str:
        """ Check if account_name matches the order account name """
        self.logger.debug('DBStore._certificate_account_check(%s)', account_name)
        result = None

        if account_name:
            # Compare the account_name from the order dictionary with the given account name
            if order_dic.get('account__name') == account_name:
                result = certificate_dic.get('order__name')
                self.logger.debug('Message signed with account key.')
            else:
                self.logger.debug('account_name and order account_name differ.')
        else:
            # If no account name given, message is signed with domain key
            result = certificate_dic.get('order__name')
            self.logger.debug('Message signed with domain key.')

        self.logger.debug('DBStore._certificate_account_check() ended with: %s', result)
        return result

    def _certificate_insert(self, data_dic: Dict[str, str]) -> str:
        """ Insert a certificate into the Certificate collection """
        self.logger.debug('_certificate_insert() for %s', data_dic.get('name', 'N/A'))

        # Convert order name to id
        try:
            order_doc = self._order_search('name', data_dic['order'])
            data_dic['order'] = order_doc['_id']
        except Exception:
            data_dic['order'] = None

        # Ensure fields are provided with default values if necessary
        data_dic.setdefault('csr', '')
        data_dic.setdefault('header_info', '')
        data_dic.setdefault('error', '')

        # Insert the data into the Certificate collection
        try:
            result = self.db['Certificate'].insert_one(data_dic)
            inserted_id = result.inserted_id
            self.logger.debug('Inserted new entry for %s with ID: %s', data_dic['name'], inserted_id)
        except Exception as err:
            self.logger.error('Failed to insert certificate for %s with error: %s', data_dic['name'], err)
            inserted_id = None

        self.logger.debug('_certificate_insert() ended with: %s', inserted_id)
        return inserted_id

    def _certificate_update(self, data_dic: Dict[str, str], exists: Dict[str, str]) -> str:
        """ Update a certificate in the Certificate collection """
        self.logger.debug('_certificate_update() for %s id: %s', data_dic['name'], exists['_id'])

        # Prepare the filter to find the existing certificate by its name
        filter = {'name': exists['name']}

        # Prepare the update operation based on the data_dic dictionary
        update = {}
        
        # If error is present in data_dic, update only the error and poll_identifier fields
        if 'error' in data_dic:
            update = {
                '$set': {
                    'error': data_dic['error'],
                    'poll_identifier': data_dic.get('poll_identifier', exists.get('poll_identifier'))
                }
            }
        else:
            # Otherwise, update multiple fields in the certificate
            update_fields = {
                'cert': data_dic.get('cert', exists.get('cert')),
                'cert_raw': data_dic.get('cert_raw', exists.get('cert_raw')),
                'issue_uts': data_dic.get('issue_uts', exists.get('issue_uts')),
                'expire_uts': data_dic.get('expire_uts', exists.get('expire_uts')),
                'renewal_info': data_dic.get('renewal_info', exists.get('renewal_info')),
                'poll_identifier': data_dic.get('poll_identifier', exists.get('poll_identifier')),
                'replaced': data_dic.get('replaced', exists.get('replaced')),
                'header_info': data_dic.get('header_info', exists.get('header_info')),
                'serial': data_dic.get('serial', exists.get('serial')),
                'aki': data_dic.get('aki', exists.get('aki'))
            }
            
            # Create the update dictionary for MongoDB
            update = {'$set': update_fields}

        # Perform the update operation
        try:
            result = self.db['Certificate'].update_one(filter, update)
            # Log the number of documents modified
            self.logger.debug('_certificate_update() ended with: %s documents modified', result.modified_count)
        except Exception as err:
            self.logger.error('_certificate_update() failed with error: %s', err)
            return None

        # Return the ID of the updated document
        return exists['_id']

    def _certificate_search(self, column: str, string: str) -> Dict[str, str]:
        """ Search the Certificate collection for a certain key/value pair """
        self.logger.debug('DBStore._certificate_search(column:%s, pattern:%s)', column, string)
        
        # Prepare the filter for the query
        if column != 'order__name':
            filter = {column: {'$regex': string, '$options': 'i'}}
        else:
            filter = {'order__name': {'$regex': string, '$options': 'i'}}
        
        try:
            # Perform the search using find_one with the prepared filter
            result = self.db['Certificate'].find_one(filter)
            self.logger.debug('DBStore._certificate_search() ended with: %s', bool(result))
        except Exception as err:
            self.logger.error('DBStore._certificate_search(column:%s, pattern:%s) failed with error: %s', column, string, err)
            result = None
        
        return result

    def _challenge_search(self, column: str, string: str) -> Dict:
        """ Search the challenge collection for a certain key/value pair """
        self.logger.debug('DBStore._challenge_search(column:%s, pattern:%s)', column, string)

        try:
            # Build the pipeline for the aggregation query
            pipeline = [
                # Perform the search using a regex pattern for the specified column
                {'$match': {f'{column}': {'$regex': string, '$options': 'i'}}},
                
                # Lookup from the status collection
                {
                    '$lookup': {
                        'from': 'status',
                        'localField': 'status_id',
                        'foreignField': '_id',
                        'as': 'status'
                    }
                },
                
                # Lookup from the authorization collection
                {
                    '$lookup': {
                        'from': 'authorization',
                        'localField': 'authorization_id',
                        'foreignField': '_id',
                        'as': 'authorization'
                    }
                },
                
                # Unwind the authorization field to flatten the results
                {'$unwind': '$authorization'},
                
                # Lookup from the orders collection
                {
                    '$lookup': {
                        'from': 'orders',
                        'localField': 'authorization.order_id',
                        'foreignField': '_id',
                        'as': 'authorization.order'
                    }
                },
                
                # Unwind the order field to flatten the results
                {'$unwind': '$authorization.order'},
                
                # Lookup from the account collection
                {
                    '$lookup': {
                        'from': 'account',
                        'localField': 'authorization.order.account_id',
                        'foreignField': '_id',
                        'as': 'authorization.order.account'
                    }
                },
                
                # Unwind the account field to flatten the results
                {'$unwind': '$authorization.order.account'},
            ]
            
            # Execute the aggregation pipeline and retrieve the first document
            result = list(self.db['challenge'].aggregate(pipeline))
            
            if result:
                # Return the first result if the list is not empty
                result = result[0]
            else:
                # If the list is empty, set result to None
                result = None
            
            self.logger.debug('DBStore._challenge_search() ended')
        except Exception as err:
            self.logger.error('DBStore._challenge_search(column:%s, pattern:%s) failed with err: %s', column, string, err)
            result = None
        
        return result

    def _cliaccount_search(self, column: str, string: str) -> Dict:
        """ Search the cliaccount collection for a certain key/value pair """
        self.logger.debug('DBStore._cliaccount_search(column:%s, pattern:%s)', column, string)

        try:
            # Perform the search using a regex pattern for the specified column
            result = self.db['cliaccount'].find_one({column: {'$regex': string, '$options': 'i'}})
            
            self.logger.debug('DBStore._cliaccount_search() ended with: %s', bool(result))
        except Exception as err:
            self.logger.error('DBStore._cliaccount_search(column:%s, pattern:%s) failed with err: %s', column, string, err)
            result = None
        
        return result

    def _db_open(self):
        """ Connect to MongoDB server and set the database """
        self.client = MongoClient()
        self.db = self.client[self.db_name]

    def _db_close(self):
        """ Close the MongoDB client """
        # Close the MongoDB client
        if self.client:
            self.client.close()

    def _db_create(self):
        """ Initialize the database with initial data """
        self.logger.debug('DBStore._db_create(%s)', self.db_name)

        # Insert initial status names if they don't exist
        status_collection = self.db['status']
        status_names = ['invalid', 'pending', 'ready', 'processing', 'valid', 'expired', 'deactivated', 'revoked']
        for status_name in status_names:
            # Use an upsert operation to insert status names if they don't exist
            status_collection.update_one({'name': status_name}, {'$setOnInsert': {'name': status_name}}, upsert=True)

        # Insert initial housekeeping data if needed
        housekeeping_collection = self.db['housekeeping']
        housekeeping_collection.update_one(
            {'name': 'dbversion'}, 
            {'$setOnInsert': {'name': 'dbversion', 'value': __dbversion__}}, 
            upsert=True
        )

        self.logger.debug('DBStore._db_create() ended')
    
    def _db_update_account(self):
        """ Update account collection in MongoDB """
        self.logger.debug('DBStore._db_update_account()')

        # Add eab_kid field to documents in the account collection, if it doesn't already exist
        self.db['account'].update_many(
            {},
            {'$set': {'eab_kid': ''}},
            upsert=True
        )

        self.logger.info('Added eab_kid field to account collection')
    
    def _db_update_authorization(self):
        """ Update authorization collection in MongoDB """
        self.logger.debug('DBStore._db_update_authorization()')

        # No explicit changes needed to the collection schema as MongoDB is schema-less
        # Use update_many to change the field type as necessary
        # You can perform data type conversion on the value field as needed

        self.logger.info('Authorization collection does not require explicit schema updates')
    
    def _db_update_cahandler(self):
        """ Update cahandler collection in MongoDB """
        self.logger.debug('DBStore._db_update_cahandler()')

        # Use insert_one or update_many as necessary to add or modify documents in the cahandler collection
        self.logger.info('Updated cahandler collection')
    
    def _db_update_certificate(self):
        """ Update certificate collection in MongoDB """
        self.logger.debug('DBStore._db_update_certificate()')

        # Use update_many to add fields as necessary to documents in the certificate collection
        # For instance:
        # self.db['certificate'].update_many({}, {'$set': {'new_field': None}}, upsert=True)

        self.logger.info('Updated certificate collection')
    
    def _db_update_challenge(self):
        """ Update challenge collection in MongoDB """
        self.logger.debug('DBStore._db_update_challenge()')

        # Use update_many to add fields as necessary to documents in the challenge collection
        self.logger.info('Updated challenge collection')
    
    def _db_update_cliaccount(self):
        """ Update cliaccount collection in MongoDB """
        self.logger.debug('DBStore._db_update_cliaccount()')

        # Use update_many to add fields as necessary to documents in the cliaccount collection
        self.logger.info('Updated cliaccount collection')

    def _db_update_housekeeping(self):
        """ Update housekeeping collection in MongoDB """
        self.logger.debug('DBStore._db_update_housekeeping()')

        # No need to check for the existence of the collection since MongoDB creates collections on the fly

        # Insert or update initial housekeeping documents as needed
        housekeeping_collection = self.db['housekeeping']

        # Ensure "name" field has a length of 30
        # No explicit schema changes needed, since MongoDB automatically handles different lengths

        self.logger.info('Housekeeping collection does not require explicit schema updates')

    def _db_update_orders(self):
        """ Update orders collection in MongoDB """
        self.logger.debug('DBStore._db_update_orders()')

        # No explicit schema changes needed; MongoDB handles field types dynamically

        self.logger.info('Orders collection does not require explicit schema updates')

    def _db_update_status(self):
        """ Update status collection in MongoDB """
        self.logger.debug('DBStore._db_update_status()')

        status_collection = self.db['status']
        # Define new statuses to add
        new_statuses = ['deactivated', 'expired', 'revoked']

        # Iterate through each status and add it if it doesn't exist
        for status in new_statuses:
            # Use upsert to add the status only if it doesn't already exist
            status_collection.update_one(
                {'name': status},
                {'$setOnInsert': {'name': status}},
                upsert=True
            )

        self.logger.info('Updated status collection')

    def _order_search(self, column: str, string: str) -> Dict:
        """ Search the orders collection for a certain key/value pair """
        self.logger.debug('DBStore._order_search(column:%s, pattern:%s)', column, string)

        try:
            # Use a filter to search for the specified column and string pattern
            filter = {column: {'$regex': string, '$options': 'i'}}
            
            # Perform the search using find_one with the filter
            result = self.db['orders'].find_one(filter)

            self.logger.debug('DBStore._order_search() ended with: %s', bool(result))
        except Exception as err:
            self.logger.error('DBStore._order_search(column:%s, pattern:%s) failed with err: %s', column, string, err)
            result = None
        
        return result

    def _status_search(self, column: str, string: str) -> Tuple[Dict, bool]:
        """ Search the status collection for a certain key/value pair """
        self.logger.debug('DBStore._status_search(column:%s, pattern:%s)', column, string)

        try:
            # Perform the search using MongoDB find_one with a filter
            result = self.db['status'].find_one({column: {'$regex': string, '$options': 'i'}})
            exists = bool(result)  # Convert the result to a boolean indicating existence
        except Exception as err:
            self.logger.error('DBStore._status_search(column:%s, pattern:%s) failed with err: %s', column, string, err)
            result = None
            exists = False

        self.logger.debug('DBStore._status_search() ended with: %s', exists)
        return (result, exists)

    def account_add(self, data_dic):
        """ Add or update an account in the account collection """
        self.logger.debug('DBStore.account_add(%s)', data_dic)

        # Add eab_kid field if not present
        if 'eab_kid' not in data_dic:
            data_dic['eab_kid'] = ''

        # Check if the account already exists based on jwk
        existing_account = self.db['account'].find_one({'jwk': data_dic['jwk']})
        
        if existing_account:
            # Update the existing account
            self.logger.debug('Account exists: %s id: %s', existing_account['name'], existing_account['_id'])
            self.db['account'].update_one(
                {'jwk': data_dic['jwk']},
                {'$set': {'alg': data_dic['alg'], 'contact': data_dic['contact']}}
            )
            aname = existing_account['name']
            created = False
        else:
            # Insert a new account
            self.db['account'].insert_one(data_dic)
            aname = data_dic['name']
            created = True

        self.logger.debug('DBStore.account_add() ended')
        return (aname, created)

    def account_delete(self, aname: str) -> bool:
        """ Delete an account from the account collection """
        self.logger.debug('DBStore.account_delete(%s)', aname)

        # Perform the delete operation using MongoDB
        delete_result = self.db['account'].delete_one({'name': aname})

        result = delete_result.deleted_count > 0
        self.logger.debug('DBStore.account_delete() ended')
        return result

    def account_lookup(self, column: str, string: str) -> Dict:
        """ Lookup the account collection for a certain key/value pair and return the result """
        self.logger.debug('DBStore.account_lookup(column:%s, pattern:%s)', column, string)
        
        try:
            # Perform the lookup using find_one
            account_data = self.db['account'].find_one({column: {'$regex': string, '$options': 'i'}})
            result = account_data if account_data else {}

            if 'created_at' in result:
                result['created_at'] = datestr_to_date(result['created_at'], '%Y-%m-%d %H:%M:%S')
        except Exception as err:
            self.logger.error('DBStore.account_lookup(column:%s, pattern:%s) failed with err: %s', column, string, err)
            result = {}

        self.logger.debug('DBStore.account_lookup() ended')
        return result

    def account_update(self, data_dic: Dict) -> str:
        """ Update an existing account in the account collection """
        self.logger.debug('DBStore.account_update(%s)', data_dic)

        # Lookup the account by name
        lookup = self.db['account'].find_one({'name': data_dic['name']})

        if lookup:
            # Fill missing fields with existing data
            data_dic.setdefault('alg', lookup['alg'])
            data_dic.setdefault('contact', lookup['contact'])
            data_dic.setdefault('jwk', lookup['jwk'])

            # Update the account
            self.db['account'].update_one(
                {'name': data_dic['name']},
                {'$set': {'alg': data_dic['alg'], 'contact': data_dic['contact'], 'jwk': data_dic['jwk']}}
            )

            result = lookup['_id']
        else:
            result = None

        self.logger.debug('DBStore.account_update() ended')
        return result

    def accountlist_get(self) -> Tuple[List[str], List[Dict]]:
        """ Get account list from MongoDB collections """
        self.logger.debug('DBStore.accountlist_get()')

        # Define the projection fields and result list
        vlist = [
            'id', 'name', 'eab_kid', 'contact', 'created_at', 'jwk', 'alg',
            'order__id', 'order__name', 'order__status__id', 'order__status__name',
            'order__notbefore', 'order__notafter', 'order__expires',
            'order__identifiers', 'order__authorization__id', 'order__authorization__name',
            'order__authorization__type', 'order__authorization__value', 'order__authorization__expires',
            'order__authorization__token', 'order__authorization__created_at',
            'order__authorization__status__id', 'order__authorization__status__name',
            'order__authorization__challenge__id', 'order__authorization__challenge__name',
            'order__authorization__challenge__token', 'order__authorization__challenge__expires',
            'order__authorization__challenge__type', 'order__authorization__challenge__keyauthorization',
            'order__authorization__challenge__created_at', 'order__authorization__challenge__status__id',
            'order__authorization__challenge__status__name'
        ]

        # Build the aggregation pipeline
        pipeline = [
            {
                '$lookup': {
                    'from': 'orders',
                    'localField': '_id',
                    'foreignField': 'account_id',
                    'as': 'orders'
                }
            },
            # Unwind the orders array to process each order individually
            {'$unwind': '$orders'},
            # Look up additional data and unwind as needed
            {
                '$lookup': {
                    'from': 'authorization',
                    'localField': 'orders._id',
                    'foreignField': 'order_id',
                    'as': 'orders.authorization'
                }
            },
            {'$unwind': '$orders.authorization'},
            {
                '$lookup': {
                    'from': 'challenge',
                    'localField': 'orders.authorization._id',
                    'foreignField': 'authorization_id',
                    'as': 'orders.authorization.challenge'
                }
            },
            {'$unwind': '$orders.authorization.challenge'},
            {
                '$lookup': {
                    'from': 'status',
                    'localField': 'orders.status_id',
                    'foreignField': '_id',
                    'as': 'orders.status'
                }
            },
            {'$unwind': '$orders.status'},
            # Same for other joins: authorization status, challenge status, etc.
            {
                '$lookup': {
                    'from': 'status',
                    'localField': 'orders.authorization.status_id',
                    'foreignField': '_id',
                    'as': 'orders.authorization.status'
                }
            },
            {'$unwind': '$orders.authorization.status'},
            {
                '$lookup': {
                    'from': 'status',
                    'localField': 'orders.authorization.challenge.status_id',
                    'foreignField': '_id',
                    'as': 'orders.authorization.challenge.status'
                }
            },
            {'$unwind': '$orders.authorization.challenge.status'},
            # Project the fields according to vlist
            {'$project': {field: 1 for field in vlist}}
        ]

        # Execute the aggregation pipeline
        rows = list(self.db['account'].aggregate(pipeline))

        self.logger.debug('DBStore.accountlist_get() ended')
        return (vlist, rows)

    def authorization_add(self, data_dic: Dict[str, str]) -> str:
        """ Add authorization to the database """
        self.logger.debug('DBStore.authorization_add(%s)', data_dic)

        try:
            # Insert the document into the authorization collection
            result = self.db['authorization'].insert_one(data_dic)
            rid = str(result.inserted_id)
        except Exception as err:
            self.logger.error('DBStore.authorization_add() failed with err: %s', err)
            rid = None

        self.logger.debug('DBStore.authorization_add() ended with: %s', rid)
        return rid

    def authorization_lookup(self, column: str, string: str, vlist: List[str] = ('type', 'value')) -> List[Dict]:
        """ Lookup authorization for a given key/value pair """
        self.logger.debug('DBStore.authorization_lookup(column:%s, pattern:%s)', column, string)

        try:
            # Use the _authorization_search function to perform the search
            lookup = self._authorization_search(column, string)
        except Exception:
            lookup = []
        
        print(lookup,"lookup found")
        
        authz_list = []
        # for row in lookup:
        #     row_dic = dict_from_row(row)
        #     tmp_dic = {ele: row_dic.get(ele) for ele in vlist}
        #     authz_list.append(tmp_dic)

        self.logger.debug('DBStore.authorization_lookup() ended')
        #return authz_list
        return lookup

    from typing import List, Dict

    def authorizations_expired_search(self, column: str, string: str, vlist: List[str] = ('id', 'name', 'expires', 'value', 'created_at', 'token', 'status__id', 'status__name', 'order__id', 'order__name'), operant='LIKE') -> List[Dict]:
        """Search for authorizations with certain criteria that are not expired."""
        self.logger.debug('DBStore.authorizations_expired_search(column:%s, pattern:%s)', column, string)

        try:
            print("try block authz ")
            
            # Initialize the aggregation pipeline with lookups
            pipeline = [
                {
                    '$lookup': {
                        'from': 'status',
                        'localField': 'status_id',
                        'foreignField': '_id',
                        'as': 'status'
                    }
                },
                {'$unwind': '$status'},
                {
                    '$lookup': {
                        'from': 'orders',
                        'localField': 'order_id',
                        'foreignField': '_id',
                        'as': 'orders'
                    }
                },
                {'$unwind': '$orders'},
            ]
            
            # Filter for the specified criteria
            match_criteria = {
                'status.name': {'$not': {'$regex': 'expired', '$options': 'i'}}
            }
            
            # Add appropriate match conditions based on the column and string
            if column == 'expires':
                # If the column is 'expires', use direct equality check
                match_criteria['expires'] = int(string)
            else:
                # For other columns, use regex match
                match_criteria[column] = {'$regex': string, '$options': 'i'}
            
            # Add match criteria to the pipeline
            pipeline.append({'$match': match_criteria})

            # Project the fields according to vlist
            pipeline.append({'$project': {ele: 1 for ele in vlist}})

            # Execute the aggregation pipeline
            rows = list(self.authorization_collection.aggregate(pipeline))

            # Convert rows to dictionary format
            authorization_list = [dict_from_row(row) for row in rows]
        except Exception as err:
            self.logger.error('DBStore.authorizations_expired_search(column:%s, pattern:%s) failed with err: %s', column, string, err)
            authorization_list = []

        self.logger.debug('DBStore.authorizations_expired_search() ended')
        return authorization_list


    def authorization_update(self, data_dic: Dict[str, str]) -> str:
        """ Update existing authorization """
        self.logger.debug('DBStore.authorization_update(%s)', data_dic)

        # Lookup the authorization by name
        lookup = self._authorization_search('name', data_dic['name'])
        print(lookup,"auth update printed lookpup search")
        if lookup:
            lookup = lookup[0]
            print(lookup,"auth update printed lookpup")
            # Check if status_id is present in the lookup result
            if 'status_id' not in lookup:
                self.logger.error('DBStore.authorization_update() failed: status_id not found in lookup result')
                return None
            
            lookup = dict_from_row(lookup)
            
            # Update status ID if specified, otherwise use existing status ID
            if 'status' in data_dic:
                status = self._status_search('name', data_dic['status'])
                if not status:
                    self.logger.error('DBStore.authorization_update() failed: status not found')
                    return None
                data_dic['status'] = dict_from_row(status)['id']
            else:
                data_dic['status'] = lookup['status_id']

            # Fill in missing fields with existing data
            data_dic.setdefault('token', lookup.get('token'))
            data_dic.setdefault('expires', lookup.get('expires'))

            try:
                # Update the authorization document
                self.db['authorization'].update_one(
                    {'name': data_dic['name']},
                    {'$set': {
                        'status_id': data_dic['status'],
                        'token': data_dic['token'],
                        'expires': data_dic['expires']
                    }}
                )

                # Retrieve the ID of the updated document
                updated_auth = self.db['authorization'].find_one({'name': data_dic['name']})
                if updated_auth:
                    result = str(updated_auth['_id'])
                else:
                    self.logger.error('DBStore.authorization_update() failed: updated document not found')
                    result = None
            except Exception as err:
                self.logger.error('DBStore.authorization_update() failed with err: %s', err)
                result = None
        else:
            self.logger.error('DBStore.authorization_update() failed: authorization not found')
            result = None

        self.logger.debug('DBStore.authorization_update() ended')
        return result


    def certificate_account_check(self, account_name: str, certificate: str) -> List[str]:
        """ Check issuer against certificate """
        self.logger.debug('DBStore.certificate_account_check(%s)', account_name)

        # Search certificate collection for the certificate
        certificate_dic = self.certificate_lookup('cert_raw', certificate, ['name', 'order__name'])

        result = None

        # Search order collection for the account name based on order name from certificate
        if 'order__name' in certificate_dic:
            order_dic = self.order_lookup('name', certificate_dic['order__name'], ['name', 'account__name'])
            if order_dic and 'account__name' in order_dic:
                # Check if the account name matches
                result = self._certificate_account_check(account_name, certificate_dic, order_dic)
            else:
                self.logger.debug('Account name missing in order dictionary')
        else:
            self.logger.debug('Certificate dictionary empty')

        self.logger.debug('DBStore.certificate_account_check() ended with: %s', result)
        return result

    def cahandler_add(self, data_dic: Dict[str, str]) -> str:
        """ Add or update cahandler in the database """
        self.logger.debug('DBStore.cahandler_add(%s)', data_dic)
        
        if 'value2' not in data_dic:
            data_dic['value2'] = ''
        
        # Check if the entry already exists
        exists = self.cahandler_lookup('name', data_dic['name'], ['id', 'name'])
        
        if exists:
            # Update the existing cahandler document
            self.logger.debug(f'cahandler exists: name id: {data_dic["name"]}')
            self.db['cahandler'].update_one(
                {'name': data_dic['name']},
                {'$set': {'value1': data_dic['value1'], 'value2': data_dic['value2']}}
            )
            rid = exists['id']
        else:
            # Insert a new cahandler document
            result = self.db['cahandler'].insert_one(data_dic)
            rid = str(result.inserted_id)
        
        self.logger.debug('DBStore.cahandler_add() ended with: %s', rid)
        return rid

    def cahandler_lookup(self, column: str, string: str, vlist: List[str] = ['name', 'value1', 'value2', 'created_at']) -> Dict[str, str]:
        """ Lookup cahandler collection for a certain key/value pair """
        self.logger.debug('DBStore.cahandler_lookup(column:%s, pattern:%s)', column, string)

        try:
            # Perform the lookup using find_one with a filter and projection
            lookup = self.db['cahandler'].find_one(
                {column: {'$regex': string, '$options': 'i'}},
                {field: 1 for field in vlist}
            )
        except Exception as err:
            self.logger.error('DBStore.cahandler_lookup() failed with err: %s', err)
            lookup = None
        
        if lookup:
            result = {ele: lookup.get(ele) for ele in vlist}
        else:
            result = {}
        
        self.logger.debug('DBStore.cahandler_lookup() ended')
        return result

    def cliaccount_add(self, data_dic: Dict[str, str]) -> str:
        """ Add or update cliaccount in the database """
        self.logger.debug('DBStore.cliaccount_add(%s)', data_dic['name'])

        # Check if the entry already exists
        exists = self._cliaccount_search('name', data_dic['name'])

        if exists:
            self.logger.debug('cliaccount exists: name id: %s', data_dic['name'])

            # Update the existing cliaccount document
            update_data = {
                'contact': data_dic.get('contact', exists.get('contact')),
                'jwk': data_dic.get('jwk', exists.get('jwk')),
                'reportadmin': data_dic.get('reportadmin'),
                'cliadmin': data_dic.get('cliadmin'),
                'certificateadmin': data_dic.get('certificateadmin')
            }
            self.db['cliaccount'].update_one(
                {'name': data_dic['name']},
                {'$set': update_data}
            )
            rid = str(exists['_id'])
        else:
            # Insert a new cliaccount document
            result = self.db['cliaccount'].insert_one(data_dic)
            rid = str(result.inserted_id)
        
        self.logger.debug('DBStore.cliaccount_add() ended with: %s', rid)
        return rid

    def cliaccount_delete(self, data_dic: Dict[str, str]) -> bool:
        """ Delete a cliaccount from the cliaccount collection """
        self.logger.debug('DBStore.cliaccount_delete(%s)', data_dic['name'])

        # Check if the entry exists
        exists = self._cliaccount_search('name', data_dic['name'])
        
        if exists:
            try:
                # Perform the delete operation
                delete_result = self.db['cliaccount'].delete_one({'name': data_dic['name']})
                result = delete_result.deleted_count > 0
            except Exception as err:
                self.logger.error('DBStore.cliaccount_delete() failed with err: %s', err)
                result = False
        else:
            self.logger.error('DBStore.cliaccount_delete() failed for name: %s', data_dic['name'])
            result = False
        
        self.logger.debug('DBStore.cliaccount_delete() ended')
        return result

    def cliaccountlist_get(self) -> List[Dict]:
        """ Retrieve the cliaccount list from MongoDB """
        self.logger.debug('DBStore.cliaccountlist_get()')

        # Define the projection fields and vlist list
        vlist = ['id', 'name', 'jwk', 'contact', 'created_at', 'cliadmin', 'reportadmin', 'certificateadmin']

        try:
            # Perform the query and project the fields according to vlist
            rows = list(self.db['cliaccount'].find({}, {field: 1 for field in vlist}))
            
            # Process results and return as a list of dictionaries
            cliaccount_list = [row for row in rows]
        except Exception as err:
            self.logger.error('DBStore.cliaccountlist_get() failed with err: %s', err)
            cliaccount_list = []

        self.logger.debug('DBStore.cliaccountlist_get() ended')
        return cliaccount_list

    def certificate_add(self, data_dic: Dict[str, str]) -> str:
        """ Add or update certificate in the certificate collection """
        self.logger.debug('DBStore.certificate_add(%s)', data_dic['name'])

        # Check if the entry already exists
        exists = self._certificate_search('name', data_dic['name'])

        if exists:
            # Fill in missing fields with existing data
            data_dic.setdefault('poll_identifier', exists.get('poll_identifier'))
            data_dic.setdefault('renewal_info', exists.get('renewal_info'))
            data_dic.setdefault('header_info', exists.get('header_info'))
            data_dic.setdefault('aki', exists.get('aki'))
            data_dic.setdefault('serial', exists.get('serial'))
            
            # Update the existing certificate
            rid = self._certificate_update(data_dic, exists)
        else:
            # Insert a new certificate
            rid = self._certificate_insert(data_dic)

        self.logger.debug('DBStore.certificate_add() ended with: %s', rid)
        return rid

    def certificate_delete(self, mkey: str, string: str) -> bool:
        """ Delete a certificate from the certificate collection """
        self.logger.debug('DBStore.certificate_delete(%s:%s)', mkey, string)

        try:
            # Perform the delete operation using MongoDB
            delete_result = self.db['certificate'].delete_one({mkey: string})
            result = delete_result.deleted_count > 0
        except Exception as err:
            self.logger.error('DBStore.certificate_delete() failed with err: %s', err)
            result = False
        
        self.logger.debug('DBStore.certificate_delete() ended')
        return result

    def certificatelist_get(self) -> Tuple[List[str], List[Dict]]:
        """ Retrieve the certificate list from MongoDB """
        self.logger.debug('DBStore.certificatelist_get()')

        # Define the fields list and vlist list
        vlist = [
            'id', 'name', 'cert_raw', 'csr', 'poll_identifier', 'created_at', 'issue_uts', 'expire_uts',
            'order__id', 'order__name', 'order__status__name', 'order__notbefore', 'order__notafter', 'order__expires',
            'order__identifiers', 'order__account__name', 'order__account__contact', 'order__account__created_at',
            'order__account__jwk', 'order__account__alg', 'order__account__eab_kid'
        ]

        try:
            # Build the aggregation pipeline
            pipeline = [
                {
                    '$lookup': {
                        'from': 'orders',
                        'localField': 'order_id',
                        'foreignField': '_id',
                        'as': 'order'
                    }
                },
                {'$unwind': '$order'},
                {
                    '$lookup': {
                        'from': 'account',
                        'localField': 'order.account_id',
                        'foreignField': '_id',
                        'as': 'order.account'
                    }
                },
                {'$unwind': '$order.account'},
                # Project the fields according to vlist
                {'$project': {field: '$' + field.replace('__', '.') for field in vlist}}
            ]

            # Execute the aggregation pipeline
            rows = list(self.db['certificate'].aggregate(pipeline))

            # Process results and return as a list of dictionaries
            cert_list = [row for row in rows]
        except Exception as err:
            self.logger.error('DBStore.certificatelist_get() failed with err: %s', err)
            cert_list = []

        self.logger.debug('DBStore.certificatelist_get() ended')
        return (vlist, cert_list)

    def certificate_lookup(self, column: str, string: str, vlist: List[str] = ('name', 'csr', 'cert', 'order__name')) -> Dict[str, str]:
        """ Search certificate collection based on a specified key/value pair """
        self.logger.debug('DBStore.certificate_lookup(%s:%s)', column, string)

        try:
            # Use the find_one method to search for the specified column and pattern
            projection = {field: 1 for field in vlist}
            lookup = self.db['certificate'].find_one({column: {'$regex': string, '$options': 'i'}}, projection)
        except Exception as err:
            self.logger.error('DBStore.certificate_lookup() failed with err: %s', err)
            lookup = None

        # Process the result
        result = {field: lookup[field] for field in vlist} if lookup else {}

        self.logger.debug('DBStore.certificate_lookup() ended with: %s', result)
        return result

    def certificates_search(self, column: str, string: str, vlist: List[str] = ('name', 'csr', 'cert', 'order__name'), operant='LIKE') -> List[Dict]:
        """ Search certificate collection for a certain key/value pair """
        self.logger.debug('DBStore.certificates_search(column:%s, pattern:%s)', column, string)

        # Use the aggregation pipeline to perform the search
        pipeline = [
            {
                '$lookup': {
                    'from': 'orders',
                    'localField': 'order_id',
                    'foreignField': '_id',
                    'as': 'order'
                }
            },
            {'$unwind': '$order'},
            {
                '$lookup': {
                    'from': 'account',
                    'localField': 'order.account_id',
                    'foreignField': '_id',
                    'as': 'order.account'
                }
            },
            {'$unwind': '$order.account'},
            {
                '$match': {
                    column: {'$regex': string, '$options': 'i'}
                }
            },
            # Project the fields according to vlist
            {'$project': {field: '$' + field.replace('__', '.') for field in vlist}}
        ]

        try:
            # Execute the aggregation pipeline
            rows = list(self.db['certificate'].aggregate(pipeline))

            # Process the results and return as a list of dictionaries
            cert_list = [row for row in rows]
        except Exception as err:
            self.logger.error('DBStore.certificates_search() failed with err: %s', err)
            cert_list = []

        self.logger.debug('DBStore.certificates_search() ended')
        return cert_list

    def challenges_search(self, column: str, string: str, vlist: List[str] = ('name', 'type', 'status__name', 'token')) -> List[Dict]:
        """ Search challenge collection for a certain key/value pair """
        self.logger.debug('DBStore.challenges_search(column:%s, pattern:%s)', column, string)

        # Use the aggregation pipeline to perform the search
        pipeline = [
            {
                '$lookup': {
                    'from': 'status',
                    'localField': 'status_id',
                    'foreignField': '_id',
                    'as': 'status'
                }
            },
            {'$unwind': '$status'},
            {
                '$lookup': {
                    'from': 'authorization',
                    'localField': 'authorization_id',
                    'foreignField': '_id',
                    'as': 'authorization'
                }
            },
            {'$unwind': '$authorization'},
            {
                '$lookup': {
                    'from': 'orders',
                    'localField': 'authorization.order_id',
                    'foreignField': '_id',
                    'as': 'authorization.order'
                }
            },
            {'$unwind': '$authorization.order'},
            {
                '$lookup': {
                    'from': 'account',
                    'localField': 'authorization.order.account_id',
                    'foreignField': '_id',
                    'as': 'authorization.order.account'
                }
            },
            {'$unwind': '$authorization.order.account'},
            {
                '$match': {
                    column: {'$regex': string, '$options': 'i'}
                }
            },
            # Project the fields according to vlist
            {'$project': {field: '$' + field.replace('__', '.') for field in vlist}}
        ]

        try:
            # Execute the aggregation pipeline
            rows = list(self.db['challenge'].aggregate(pipeline))

            # Process the results and return as a list of dictionaries
            challenge_list = [row for row in rows]
        except Exception as err:
            self.logger.error('DBStore.challenges_search() failed with err: %s', err)
    
    def challenge_add(self, value: str, mtype: str, data_dic: Dict[str, str]) -> str:
        """ Add challenge to the challenge collection """
        self.logger.debug('DBStore.challenge_add(%s:%s)', value, mtype)

        # Lookup the authorization based on its name
        authorization = self.authorization_lookup('name', data_dic['authorization'], ['_id'])

        if authorization:
            data_dic['authorization'] = authorization['_id']
            data_dic.setdefault('status', 2)

            try:
                # Perform the insert operation
                result = self.db['challenge'].insert_one(data_dic)
                rid = str(result.inserted_id)
            except Exception as err:
                self.logger.error('DBStore.challenge_add() failed with err: %s', err)
                rid = None
        else:
            self.logger.error('DBStore.challenge_add() failed: authorization not found for %s', data_dic['authorization'])
            rid = None

        self.logger.debug('DBStore.challenge_add() ended with: %s', rid)
        return rid

    def challenge_lookup(self, column: str, string: str, vlist: List[str] = ('type', 'token', 'status__name')) -> Dict[str, str]:
        """ Search challenge collection for a given key/value pair """
        self.logger.debug('DBStore.challenge_lookup(%s:%s)', column, string)

        try:
            # Perform the lookup using find_one with a filter and projection
            projection = {field: 1 for field in vlist}
            lookup = self.db['challenge'].find_one({column: {'$regex': string, '$options': 'i'}}, projection)
        except Exception as err:
            self.logger.error('DBStore.challenge_lookup() failed with err: %s', err)
            lookup = None

        # Process the result
        result = {field: lookup[field] for field in vlist} if lookup else {}
        if 'status__name' in vlist and 'status' in result:
            result['status'] = lookup['status__name']

        self.logger.debug('DBStore.challenge_lookup() ended with: %s', result)
        return result

    def challenge_update(self, data_dic: Dict[str, str]) -> None:
        """ Update existing challenge """
        self.logger.debug('DBStore.challenge_update(%s)', data_dic)

        # Lookup the challenge by name
        lookup = self._challenge_search('name', data_dic['name'])

        if lookup:
            lookup = dict_from_row(lookup)
            
            # Update status ID if specified, otherwise use existing status ID
            if 'status' in data_dic:
                status = self._status_search('name', data_dic['status'])
                data_dic['status'] = dict_from_row(status)['id']
            else:
                data_dic['status'] = lookup['status__id']

            # Fill in missing fields with existing data
            data_dic.setdefault('keyauthorization', lookup['keyauthorization'])
            data_dic.setdefault('validated', lookup['validated'])

            # Update the challenge document
            try:
                self.db['challenge'].update_one(
                    {'name': data_dic['name']},
                    {'$set': {
                        'status_id': data_dic['status'],
                        'keyauthorization': data_dic['keyauthorization'],
                        'validated': data_dic['validated']
                    }}
                )
            except Exception as err:
                self.logger.error('DBStore.challenge_update() failed with err: %s', err)

        self.logger.debug('DBStore.challenge_update() ended')

    def cli_jwk_load(self, aname: str) -> Dict[str, str]:
        """ Load cliaccount information and build jwk key dictionary """
        self.logger.debug('DBStore.cli_jwk_load(%s)', aname)

        try:
            # Lookup the cliaccount based on the provided name
            account = self._cliaccount_search('name', aname)
            
            # Extract the jwk field from the account dictionary and load it as JSON
            jwk_dict = json.loads(account['jwk']) if account else {}
        except Exception as err:
            self.logger.error('DBStore.cli_jwk_load() failed with err: %s', err)
            jwk_dict = {}

        self.logger.debug('DBStore.cli_jwk_load() ended with: %s', jwk_dict)
        return jwk_dict

    def cli_permissions_get(self, aname: str) -> Dict[str, str]:
        """ Load cliaccount permissions information """
        self.logger.debug('DBStore.cli_permissions_get(%s)', aname)

        try:
            # Lookup the cliaccount based on the provided name
            account = self._cliaccount_search('name', aname)
            
            # Extract the permissions from the account dictionary
            account_dic = {
                'cliadmin': account.get('cliadmin', 0),
                'reportadmin': account.get('reportadmin', 0),
                'certificateadmin': account.get('certificateadmin', 0)
            } if account else {}
        except Exception as err:
            self.logger.error('DBStore.cli_permissions_get() failed with err: %s', err)
            account_dic = {}

        self.logger.debug('DBStore.cli_permissions_get() ended')
        return account_dic

    def db_update(self):
        """ Update database """
        self.logger.debug('DBStore.db_update()')
        
        # Perform any necessary updates to collections as needed
        # Call update functions for each collection (e.g., `self._db_update_certificate()`)
        self._db_update_certificate()
        self._db_update_status()
        self._db_update_challenge()
        self._db_update_account()
        self._db_update_orders()
        self._db_update_authorization()
        self._db_update_housekeeping()
        self._db_update_cahandler()
        self._db_update_cliaccount()

        # Update db version in housekeeping collection
        try:
            self.db['housekeeping'].update_one(
                {'name': 'dbversion'},
                {'$set': {'value': __dbversion__}},
                upsert=True
            )
            self.logger.info(f'Updated dbversion to {__dbversion__}')
        except Exception as err:
            self.logger.error('DBStore.db_update() failed with err: %s', err)

        self.logger.debug('DBStore.db_update() ended')

    def dbversion_get(self) -> Tuple[str, str]:
        """ Get database version from the housekeeping collection """
        self.logger.debug('DBStore.dbversion_get()')

        try:
            # Retrieve the dbversion from the housekeeping collection
            result = self.db['housekeeping'].find_one({'name': 'dbversion'}, {'value': 1})
            db_version = result['value'] if result else None
        except Exception as err:
            self.logger.error('DBStore.dbversion_get() failed with err: %s', err)
            db_version = None

        self.logger.debug('DBStore.dbversion_get() ended with %s', db_version)
        return db_version, 'tools/db_update.py'

    def hkparameter_add(self, data_dic: Dict[str, str]) -> Tuple[str, bool]:
        """ Add or update housekeeping parameter in the housekeeping collection """
        self.logger.debug('DBStore.hkparameter_add(%s)', data_dic)

        # Use upsert to insert or update the document based on the provided data_dic
        result = self.db['housekeeping'].update_one(
            {'name': data_dic['name']},
            {'$set': {'value': data_dic['value']}},
            upsert=True
        )

        # Determine if a new document was created
        created = result.upserted_id is not None

        self.logger.debug('DBStore.hkparameter_add() ended with: %s, created: %s', data_dic['name'], created)
        return data_dic['name'], created

    def hkparameter_get(self, parameter: str) -> str:
        """ Get parameter from the housekeeping collection """
        self.logger.debug('DBStore.hkparameter_get()')

        try:
            # Retrieve the parameter value from the housekeeping collection
            result = self.db['housekeeping'].find_one({'name': parameter}, {'value': 1})
            parameter_value = result['value'] if result else None
        except Exception as err:
            self.logger.error('DBStore.hkparameter_get() failed with err: %s', err)
            parameter_value = None

        self.logger.debug('DBStore.hkparameter_get() ended with: %s', parameter_value)
        return parameter_value

    def jwk_load(self, aname: str) -> Dict[str, str]:
        """ Load account information and build jwk key dictionary """
        self.logger.debug('DBStore.jwk_load(%s)', aname)

        try:
            # Lookup the account by name
            account = self._account_search('name', aname)

            # Extract the jwk field and load it as JSON
            jwk_dict = json.loads(account['jwk']) if account else {}
            jwk_dict['alg'] = account['alg'] if account else None
        except Exception as err:
            self.logger.error('DBStore.jwk_load() failed with err: %s', err)
            jwk_dict = {}

        self.logger.debug('DBStore.jwk_load() ended with: %s', jwk_dict)
        return jwk_dict

    def nonce_add(self, nonce: str) -> str:
        """ Add nonce to the nonce collection """
        self.logger.debug('DBStore.nonce_add(%s)', nonce)

        try:
            # Insert the nonce into the nonce collection
            result = self.db['nonce'].insert_one({'nonce': nonce})
            rid = str(result.inserted_id)
        except Exception as err:
            self.logger.error('DBStore.nonce_add() failed with err: %s', err)
            rid = None

        self.logger.debug('DBStore.nonce_add() ended with: %s', rid)
        return rid

    def nonce_check(self, nonce: str) -> bool:
        """ Check if a nonce exists in the nonce collection """
        self.logger.debug('DBStore.nonce_check(%s)', nonce)

        try:
            # Check if the nonce exists in the collection
            exists = self.db['nonce'].count_documents({'nonce': nonce}) > 0
        except Exception as err:
            self.logger.error('DBStore.nonce_check() failed with err: %s', err)
            exists = False

        self.logger.debug('DBStore.nonce_check() ended')
        return exists

    def nonce_delete(self, nonce: str):
        """ Delete nonce from the nonce collection """
        self.logger.debug('DBStore.nonce_delete(%s)', nonce)

        try:
            # Perform the delete operation using MongoDB
            self.db['nonce'].delete_one({'nonce': nonce})
        except Exception as err:
            self.logger.error('DBStore.nonce_delete() failed with err: %s', err)

        self.logger.debug('DBStore.nonce_delete() ended')

    def order_add(self, data_dic: Dict[str, str]) -> str:
        """ Add order to the orders collection """
        self.logger.debug('DBStore.order_add(%s)', data_dic)
        
        # Fill in default values for notbefore and notafter if not provided
        data_dic.setdefault('notbefore', 0)
        data_dic.setdefault('notafter', 0)

        print(data_dic)

        # Lookup the account by name
        account = self.account_lookup('name', data_dic['account'])
        print(account, "account found")
        if account:
            # Use the account ID in the data dictionary
            data_dic['account'] = account['_id']
            print("if account entered")
            try:
                print("trying inserting teh value ")
                # Perform the insert operation
                result = self.orders_collection.insert_one(data_dic)
                rid = str(result.inserted_id)
            except Exception as err:
                self.logger.error('DBStore.order_add() failed with err: %s', err)
                rid = None
        else:
            rid = None
            self.logger.error('DBStore.order_add() failed: account not found for %s', data_dic['account'])

        self.logger.debug('DBStore.order_add() ended with: %s', rid)
        return rid

    def order_lookup(self, column: str, string: str, vlist: List[str] = ('notbefore', 'notafter', 'identifiers', 'expires', 'status__name')) -> Dict[str, str]:
        """ Search orders for a given ordername """
        self.logger.debug('order_lookup(%s:%s)', column, string)

        try:
            # Perform the lookup using find_one with a filter and projection
            projection = {field: 1 for field in vlist}
            lookup = self.db['orders'].find_one({column: {'$regex': string, '$options': 'i'}}, projection)
        except Exception as err:
            self.logger.error('DBStore.order_lookup() failed with err: %s', err)
            lookup = None

        # Process the result
        if lookup:
            # Convert any empty values for notbefore and notafter to 0
            lookup['notbefore'] = lookup.get('notbefore', 0)
            lookup['notafter'] = lookup.get('notafter', 0)

            # Process the lookup to construct the result dictionary
            result = {field: lookup[field] for field in vlist}
            if 'status__name' in vlist:
                result['status'] = lookup.get('status__name')
        else:
            result = {}

        self.logger.debug('DBStore.order_lookup() ended with: %s', result)
        return result

    def order_update(self, data_dic: Dict[str, str]) -> None:
        """ Update an order in the orders collection """
        self.logger.debug('DBStore.order_update(%s)', data_dic)

        # If status is specified, convert it to a status ID
        if 'status' in data_dic:
            status = self._status_search('name', data_dic['status'])
            if status:
                data_dic['status'] = dict_from_row(status)['id']
            else:
                self.logger.error('DBStore.order_update() failed: status not found for %s', data_dic['status'])
                return
        
        try:
            # Perform the update operation using MongoDB
            self.db['orders'].update_one(
                {'name': data_dic['name']},
                {'$set': {'status_id': data_dic['status']}}
            )
        except Exception as err:
            self.logger.error('DBStore.order_update() failed with err: %s', err)

        self.logger.debug('DBStore.order_update() ended')

    def orders_invalid_search(self, column: str, string: str, vlist: List[str] = ('id', 'name', 'expires', 'identifiers', 'created_at', 'status__id', 'status__name', 'account__id', 'account__name', 'account__contact'), operant='LIKE') -> List[Dict]:
        """ Search orders collection for a certain key/value pair where orders are invalid """
        self.logger.debug('DBStore.orders_invalid_search(column:%s, pattern:%s)', column, string)

        # Build the aggregation pipeline to perform the search
        pipeline = [
            {
                '$lookup': {
                    'from': 'status',
                    'localField': 'status_id',
                    'foreignField': '_id',
                    'as': 'status'
                }
            },
            {'$unwind': '$status'},
            {
                '$lookup': {
                    'from': 'account',
                    'localField': 'account_id',
                    'foreignField': '_id',
                    'as': 'account'
                }
            },
            {'$unwind': '$account'},
            {
                '$match': {
                    'status_id': {'$gt': 1},
                    column: {'$regex': string, '$options': 'i'}
                }
            },
            # Project the fields according to vlist
            {'$project': {ele: '$' + ele.replace('__', '.') for ele in vlist}}
        ]

        try:
            # Execute the aggregation pipeline
            rows = list(self.db['orders'].aggregate(pipeline))

            # Process the results and return as a list of dictionaries
            order_list = [row for row in rows]
        except Exception as err:
            self.logger.error('DBStore.orders_invalid_search() failed with err: %s', err)
            order_list = []

        self.logger.debug('DBStore.orders_invalid_search() ended')
        return order_list

